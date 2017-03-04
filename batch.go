//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuego

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type docBackIndexRow struct {
	docID        string
	docIDBytes   []byte
	doc          *document.Document // If deletion, doc will be nil.
	backIndexRow *BackIndexRow
}

// --------------------------------------------------

type batchEntry struct {
	analyzeResult *AnalyzeAuxResult
	recId         uint64
}

// --------------------------------------------------

type batchEntryTFR struct {
	batchEntry *batchEntry

	// Index into batchEntry.analyzeResult.TermFreqRows array.
	termFreqRowIdx int
}

// --------------------------------------------------

func (udc *Fuego) Batch(batch *index.Batch) error {
	segDirtiness, err := udc.batch(batch)
	if err != nil {
		return err
	}

	return udc.Cleaner(segDirtiness)
}

func (udc *Fuego) batch(batch *index.Batch) (
	segDirtiness map[uint64]int64, err error) {
	analysisStart := time.Now()

	analyzeResultCh := make(chan *AnalyzeAuxResult, len(batch.IndexOps))

	go udc.analyzeBatch(batch, analyzeResultCh)

	docBackIndexRowCh := make(chan *docBackIndexRow, len(batch.IndexOps))
	docBackIndexRowErrCh := make(chan error)

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// Retrieve back index rows concurrent with analysis.
	go udc.fetchBatchBackIndexRows(batch,
		docBackIndexRowCh, docBackIndexRowErrCh)

	// The segId's decrease or drop downwards from MAX_UINT64,
	// which allows newer/younger seg's to appear first in iterators.
	udc.lastUsedSegId = udc.lastUsedSegId - 1

	currSegId := udc.lastUsedSegId

	// Wait for analyze results.
	numUpdates, numPlainTextBytes := countBatchSize(batch)

	batchEntriesArr, batchEntriesMap, numTermFreqRows :=
		udc.waitForAnalyzeResults(numUpdates, analyzeResultCh)

	close(analyzeResultCh)

	// NOTE: We might consider sorting the batchEntriesArr by docID,
	// ASC, in order to assign the recId's in the same sorted ordering
	// as docID's, but we'll skip this for now until we figure out if
	// there's a performance win.
	//   sort.Sort(batchEntriesArr)

	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	indexStart := time.Now()

	// Group the batchEntry's by their fieldTerm.
	groupedByFieldTerm := groupByFieldTerms(batchEntriesArr, numTermFreqRows)

	// NOTE: We might consider sorting the fieldTerms by field ASC,
	// term ASC, but skipping this for now until we can figure out if
	// there's a performance win.
	//   fieldTerms := keysAsFieldTerms(groupedByFieldTerm)
	//   sort.Sort(fieldTerms)

	// Need a summary row update.
	var addRowsAll [][]KVRow
	var updateRowsAll [][]KVRow
	var deleteRowsAll [][]KVRow

	// Add the postings.
	addRows := convertToPostings(currSegId, groupedByFieldTerm)
	if len(addRows) > 0 {
		addRowsAll = append(addRowsAll, addRows)
	}

	// Add the internal ops.
	if len(batch.InternalOps) > 0 {
		var updateRows []KVRow
		var deleteRows []KVRow

		for internalK, internalV := range batch.InternalOps {
			internalRow := NewInternalRow([]byte(internalK), internalV)
			if internalV != nil {
				updateRows = append(updateRows, internalRow)
			} else {
				deleteRows = append(deleteRows, internalRow)
			}
		}

		if len(updateRows) > 0 {
			updateRowsAll = append(updateRowsAll, updateRows)
		}
		if len(deleteRows) > 0 {
			deleteRowsAll = append(deleteRowsAll, deleteRows)
		}
	}

	// Process back index rows as they arrive.
	dictionaryDeltas, docsAdded, docsDeleted,
		addRowsAll, updateRowsAll, deleteRowsAll, segDirtiness :=
		udc.processBatchBackIndexRows(currSegId, docBackIndexRowCh,
			batchEntriesMap, addRowsAll, updateRowsAll, deleteRowsAll)

	docBackIndexRowErr := <-docBackIndexRowErrCh
	if docBackIndexRowErr != nil {
		return nil, docBackIndexRowErr
	}

	// start a writer for this batch
	kvwriter, err := udc.store.Writer()
	if err != nil {
		return nil, err
	}

	err = udc.batchRows(kvwriter,
		addRowsAll, updateRowsAll, deleteRowsAll, dictionaryDeltas)

	cerr := kvwriter.Close()
	if cerr != nil && err == nil {
		err = cerr
	}

	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))

	if err != nil {
		atomic.AddUint64(&udc.stats.errors, 1)
		return nil, err
	}

	udc.m.Lock()
	udc.docCount += docsAdded
	udc.docCount -= docsDeleted
	udc.m.Unlock()

	atomic.AddUint64(&udc.stats.updates, uint64(numUpdates))
	atomic.AddUint64(&udc.stats.deletes, docsDeleted)
	atomic.AddUint64(&udc.stats.batches, 1)
	atomic.AddUint64(&udc.stats.numPlainTextBytesIndexed, numPlainTextBytes)

	return segDirtiness, nil
}

func (udc *Fuego) deleteSingle(idBytes []byte, backIndexRow *BackIndexRow,
	dictionaryDeltas map[string]int64, keyBuf []byte) ([]KVRow, []byte) {
	deleteRows := make([]KVRow, 0, len(backIndexRow.termEntries)+len(backIndexRow.storedEntries)+3)

	var tfr TermFrequencyRow

	for _, te := range backIndexRow.termEntries {
		InitTermFrequencyRow(&tfr, []byte(*te.Term), uint16(*te.Field), idBytes, 0, 0)

		if tfr.DictionaryRowKeySize() > len(keyBuf) {
			keyBuf = make([]byte, tfr.DictionaryRowKeySize())
		}
		dictKeySize, err := tfr.DictionaryRowKeyTo(keyBuf)
		if err == nil {
			dictionaryDeltas[string(keyBuf[:dictKeySize])] -= 1
		}
	}

	for _, se := range backIndexRow.storedEntries {
		sf := NewStoredRow(idBytes, uint16(*se.Field), se.ArrayPositions, 'x', nil)
		deleteRows = append(deleteRows, sf)
	}

	deleteRows = append(deleteRows,
		NewIdRow(backIndexRow.segId, backIndexRow.recId, nil))

	// also delete the backIndexRow itself
	return append(deleteRows, backIndexRow), keyBuf
}

func (udc *Fuego) batchRows(writer store.KVWriter,
	addRowsAll [][]KVRow, updateRowsAll [][]KVRow, deleteRowsAll [][]KVRow,
	dictionaryDeltas map[string]int64) error {
	// count up bytes needed for buffering.
	addNum := 0
	addKeyBytes := 0
	addValBytes := 0

	updateNum := 0
	updateKeyBytes := 0
	updateValBytes := 0

	deleteNum := 0
	deleteKeyBytes := 0

	for _, addRows := range addRowsAll {
		for _, row := range addRows {
			udc.Logf("   addRow: %v\n", row)

			addKeyBytes += row.KeySize()
			addValBytes += row.ValueSize()
		}
		addNum += len(addRows)
	}

	for _, updateRows := range updateRowsAll {
		for _, row := range updateRows {
			udc.Logf("   updateRow: %v\n", row)

			updateKeyBytes += row.KeySize()
			updateValBytes += row.ValueSize()
		}
		updateNum += len(updateRows)
	}

	for _, deleteRows := range deleteRowsAll {
		for _, row := range deleteRows {
			udc.Logf("   deleteRow: %v\n", row)

			deleteKeyBytes += row.KeySize()
		}
		deleteNum += len(deleteRows)
	}

	mergeNum := len(dictionaryDeltas)
	mergeKeyBytes := 0
	mergeValBytes := mergeNum * DictionaryRowMaxValueSize

	for dictRowKey := range dictionaryDeltas {
		mergeKeyBytes += len(dictRowKey)
	}

	// prepare batch
	totBytes := addKeyBytes + addValBytes +
		updateKeyBytes + updateValBytes +
		deleteKeyBytes +
		2*(mergeKeyBytes+mergeValBytes)

	buf, wb, err := writer.NewBatchEx(store.KVBatchOptions{
		TotalBytes: totBytes,
		NumSets:    addNum + updateNum,
		NumDeletes: deleteNum,
		NumMerges:  mergeNum,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = wb.Close()
	}()

	// fill the batch
	for _, addRows := range addRowsAll {
		for _, row := range addRows {
			keySize, err := row.KeyTo(buf)
			if err != nil {
				return err
			}
			valSize, err := row.ValueTo(buf[keySize:])
			if err != nil {
				return err
			}
			wb.Set(buf[:keySize], buf[keySize:keySize+valSize])
			buf = buf[keySize+valSize:]
		}
	}

	for _, updateRows := range updateRowsAll {
		for _, row := range updateRows {
			keySize, err := row.KeyTo(buf)
			if err != nil {
				return err
			}
			valSize, err := row.ValueTo(buf[keySize:])
			if err != nil {
				return err
			}
			wb.Set(buf[:keySize], buf[keySize:keySize+valSize])
			buf = buf[keySize+valSize:]
		}
	}

	for _, deleteRows := range deleteRowsAll {
		for _, row := range deleteRows {
			keySize, err := row.KeyTo(buf)
			if err != nil {
				return err
			}
			wb.Delete(buf[:keySize])
			buf = buf[keySize:]
		}
	}

	for dictRowKey, delta := range dictionaryDeltas {
		dictRowKeyLen := copy(buf, dictRowKey)
		binary.LittleEndian.PutUint64(buf[dictRowKeyLen:], uint64(delta))
		wb.Merge(buf[:dictRowKeyLen], buf[dictRowKeyLen:dictRowKeyLen+DictionaryRowMaxValueSize])
		buf = buf[dictRowKeyLen+DictionaryRowMaxValueSize:]
	}

	// write out the batch
	return writer.ExecuteBatch(wb)
}

func (udc *Fuego) mergeOldAndNew(segId uint64,
	backIndexRow *BackIndexRow, batchEntry *batchEntry,
	dictionaryDeltas map[string]int64, keyBuf []byte) (
	addRows []KVRow, updateRows []KVRow, deleteRows []KVRow, keyBufOut []byte) {
	ar := batchEntry.analyzeResult

	ar.BackIndexRow.segId = segId
	ar.BackIndexRow.recId = batchEntry.recId

	numRows := 2 + len(ar.FieldRows) + len(ar.TermFreqRows) + len(ar.StoredRows)

	addRows = make([]KVRow, 0, 1+numRows)
	addRows = append(addRows, NewIdRow(ar.BackIndexRow.segId, ar.BackIndexRow.recId, ar.DocIDBytes))

	if backIndexRow == nil {
		addRows = append(addRows, ar.BackIndexRow)

		for _, row := range ar.FieldRows {
			addRows = append(addRows, row)
		}

		for _, row := range ar.TermFreqRows {
			if row.DictionaryRowKeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.DictionaryRowKeySize())
			}
			dictKeySize, err := row.DictionaryRowKeyTo(keyBuf)
			if err == nil {
				dictionaryDeltas[string(keyBuf[:dictKeySize])] += 1
			}
		}

		for _, row := range ar.StoredRows {
			addRows = append(addRows, row)
		}

		return addRows, nil, nil, keyBuf
	}

	addRows = append(addRows,
		NewDeletionRow(backIndexRow.segId, backIndexRow.recId))

	updateRows = make([]KVRow, 0, numRows)
	updateRows = append(updateRows, ar.BackIndexRow)

	for _, row := range ar.FieldRows {
		updateRows = append(updateRows, row)
	}

	var existingTermKeys map[string]struct{}
	backIndexTermKeys := backIndexRow.AllTermKeys()
	if len(backIndexTermKeys) > 0 {
		existingTermKeys = make(map[string]struct{}, len(backIndexTermKeys))
		for _, key := range backIndexTermKeys {
			existingTermKeys[string(key)] = struct{}{}
		}
	}

	var existingStoredKeys map[string]struct{}
	backIndexStoredKeys := backIndexRow.AllStoredKeys()
	if len(backIndexStoredKeys) > 0 {
		existingStoredKeys = make(map[string]struct{}, len(backIndexStoredKeys))
		for _, key := range backIndexStoredKeys {
			existingStoredKeys[string(key)] = struct{}{}
		}
	}

	for _, row := range ar.TermFreqRows {
		if existingTermKeys != nil {
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}
			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingTermKeys[string(keyBuf[:keySize])]; ok {
				delete(existingTermKeys, string(keyBuf[:keySize]))
				continue
			}
		}

		if row.DictionaryRowKeySize() > len(keyBuf) {
			keyBuf = make([]byte, row.DictionaryRowKeySize())
		}
		dictKeySize, err := row.DictionaryRowKeyTo(keyBuf)
		if err == nil {
			dictionaryDeltas[string(keyBuf[:dictKeySize])] += 1
		}
	}

	for _, row := range ar.StoredRows {
		if existingStoredKeys != nil {
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}
			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingStoredKeys[string(keyBuf[:keySize])]; ok {
				updateRows = append(updateRows, row)
				delete(existingStoredKeys, string(keyBuf[:keySize]))
				continue
			}
		}

		addRows = append(addRows, row)
	}

	deleteRows = make([]KVRow, 0, 1+len(existingTermKeys)+len(existingStoredKeys))
	deleteRows = append(deleteRows,
		NewIdRow(backIndexRow.segId, backIndexRow.recId, nil))

	// any of the existing termFrequencyRows that weren't updated need to be deleted
	for existingTermKey := range existingTermKeys {
		tfr, err := NewTermFrequencyRowK([]byte(existingTermKey))
		if err == nil {
			if tfr.DictionaryRowKeySize() > len(keyBuf) {
				keyBuf = make([]byte, tfr.DictionaryRowKeySize())
			}
			dictKeySize, err := tfr.DictionaryRowKeyTo(keyBuf)
			if err == nil {
				dictionaryDeltas[string(keyBuf[:dictKeySize])] -= 1
			}
		}
	}

	// any of the existing storedRows that weren't updated need to be deleted
	for existingStoredKey := range existingStoredKeys {
		storedRow, err := NewStoredRowK([]byte(existingStoredKey))
		if err == nil {
			deleteRows = append(deleteRows, storedRow)
		}
	}

	return addRows, updateRows, deleteRows, keyBuf
}

// -------------------------------------------------------

func countBatchSize(batch *index.Batch) (
	numUpdates int, numPlainTextBytes uint64) {
	for _, doc := range batch.IndexOps {
		if doc != nil {
			numUpdates++
			numPlainTextBytes += doc.NumPlainTextBytes()
		}
	}

	return numUpdates, numPlainTextBytes
}

// -------------------------------------------------------

func (udc *Fuego) analyzeBatch(batch *index.Batch,
	analyzeResultCh chan *AnalyzeAuxResult) {
	for _, doc := range batch.IndexOps {
		if doc != nil {
			AnalyzeAuxQueue <- &AnalyzeAuxReq{
				Index:    udc,
				Doc:      doc,
				ResultCh: analyzeResultCh,
			}
		}
	}
}

func (udc *Fuego) waitForAnalyzeResults(numUpdates int,
	analyzeResultCh chan *AnalyzeAuxResult) (
	[]*batchEntry, map[string]*batchEntry, int) {
	// Prealloc'ed.
	batchEntriesPre := make([]batchEntry, numUpdates)

	// The batchEntry.recId will be the 1-based based position of the
	// batchEntry in the batchEntriesArr.
	batchEntriesArr := make([]*batchEntry, numUpdates)

	// Keyed by docID.
	batchEntriesMap := make(map[string]*batchEntry, numUpdates)

	var numBatchEntries int
	var numTermFreqRows int

	for numBatchEntries < numUpdates {
		analyzeResult := <-analyzeResultCh

		batchEntry := &batchEntriesPre[numBatchEntries]
		numBatchEntries++

		batchEntry.analyzeResult = analyzeResult
		batchEntry.recId = uint64(numBatchEntries)

		batchEntriesArr[len(batchEntriesArr)] = batchEntry
		batchEntriesMap[analyzeResult.DocID] = batchEntry

		numTermFreqRows += len(analyzeResult.TermFreqRows)
	}

	return batchEntriesArr, batchEntriesMap, numTermFreqRows
}

// -------------------------------------------------------

func groupByFieldTerms(batchEntriesArr []*batchEntry,
	numTermFreqRows int) map[fieldTerm][]*batchEntryTFR {
	groupedByFieldTerm := map[fieldTerm][]*batchEntryTFR{}

	batchEntryTFRPre := make([]batchEntryTFR, numTermFreqRows) // Prealloc'ed.
	batchEntryTFRUsed := 0

	for _, batchEntry := range batchEntriesArr {
		for tfrIdx, tfr := range batchEntry.analyzeResult.TermFreqRows {
			fieldTerm := fieldTerm{tfr.field, string(tfr.term)}

			batchEntryTFR := &batchEntryTFRPre[batchEntryTFRUsed]
			batchEntryTFRUsed += 1

			batchEntryTFR.batchEntry = batchEntry
			batchEntryTFR.termFreqRowIdx = tfrIdx

			// We're bucketing or grouping by the fieldTerm's, but
			// also keeping the overall ordering stable by being
			// driven by ordering of the batchEntriesArr.
			groupedByFieldTerm[fieldTerm] =
				append(groupedByFieldTerm[fieldTerm], batchEntryTFR)
		}
	}

	return groupedByFieldTerm
}

// -------------------------------------------------------

func convertToPostings(currSegId uint64,
	groupedByFieldTerm map[fieldTerm][]*batchEntryTFR) []KVRow {
	rv := make([]KVRow, 0, len(groupedByFieldTerm)*3)

	for fieldTerm, batchEntryTFRs := range groupedByFieldTerm {
		recIds := make([]uint64, len(batchEntryTFRs))
		freqNorms := make([]uint32, 2*len(batchEntryTFRs))
		vectors := make([][]*TermVector, len(batchEntryTFRs))

		i2 := 0
		for i, batchEntryTFR := range batchEntryTFRs {
			batchEntry := batchEntryTFR.batchEntry

			recIds[i] = batchEntry.recId

			tfrIdx := batchEntryTFR.termFreqRowIdx
			tfr := batchEntry.analyzeResult.TermFreqRows[tfrIdx]

			freqNorms[i2] = uint32(tfr.freq)
			freqNorms[i2+1] = math.Float32bits(tfr.norm)
			i2 += 2

			vectors[i] = tfr.vectors
		}

		termBytes := []byte(fieldTerm.term)

		rv = append(rv, NewPostingRecIdsRow(
			fieldTerm.field, termBytes, currSegId, recIds))

		rv = append(rv, NewPostingFreqNormsRow(
			fieldTerm.field, termBytes, currSegId, freqNorms))

		rv = append(rv, NewPostingVecsRowFromVectors(
			fieldTerm.field, termBytes, currSegId, vectors))
	}

	return rv
}

// -------------------------------------------------------

func (udc *Fuego) fetchBatchBackIndexRows(batch *index.Batch,
	rowCh chan *docBackIndexRow, errCh chan error) {
	defer close(errCh)
	defer close(rowCh)

	kvreader, err := udc.store.Reader()
	if err != nil {
		errCh <- err
		return
	}
	defer kvreader.Close()

	var tmpRow BackIndexRow

	for docID, doc := range batch.IndexOps {
		docIDBytes := []byte(docID)

		bir, err := backIndexRowForDocID(kvreader, docIDBytes, &tmpRow)
		if err != nil {
			errCh <- err
			return
		}

		rowCh <- &docBackIndexRow{docID, docIDBytes, doc, bir}
	}
}

// -------------------------------------------------------

func (udc *Fuego) processBatchBackIndexRows(
	currSegId uint64,
	docBackIndexRowCh chan *docBackIndexRow,
	batchEntriesMap map[string]*batchEntry,
	addRowsAll, updateRowsAll, deleteRowsAll [][]KVRow) (
	dictionaryDeltas map[string]int64,
	docsAdded, docsDeleted uint64,
	addRowsAllOut, updateRowsAllOut, deleteRowsAllOut [][]KVRow,
	segDirtiness map[uint64]int64) {
	var addRows []KVRow

	dictionaryDeltas = make(map[string]int64) // Keyed by dictionaryRow key.

	segDirtiness = map[uint64]int64{} // Keyed by segId.
	segDirtiness[currSegId] += 0      // Ensure an entry for currSegId.

	keyBuf := GetRowBuffer()

	for dbir := range docBackIndexRowCh {
		if dbir.doc == nil {
			if dbir.backIndexRow != nil { // A deletion.
				addRows = append(addRows,
					NewDeletionRow(dbir.backIndexRow.segId, dbir.backIndexRow.recId))

				var deleteRows []KVRow

				deleteRows, keyBuf = udc.deleteSingle(
					dbir.docIDBytes, dbir.backIndexRow, dictionaryDeltas, keyBuf)
				if len(deleteRows) > 0 {
					deleteRowsAll = append(deleteRowsAll, deleteRows)
				}

				docsDeleted++
			}
		} else {
			var aRows, uRows, dRows []KVRow

			aRows, uRows, dRows, keyBuf = udc.mergeOldAndNew(currSegId,
				dbir.backIndexRow, batchEntriesMap[dbir.docID],
				dictionaryDeltas, keyBuf)
			if len(aRows) > 0 {
				addRowsAll = append(addRowsAll, aRows)
			}
			if len(uRows) > 0 {
				updateRowsAll = append(updateRowsAll, uRows)
			}
			if len(dRows) > 0 {
				deleteRowsAll = append(deleteRowsAll, dRows)
			}

			if dbir.backIndexRow != nil {
				segDirtiness[dbir.backIndexRow.segId] += 1
			} else {
				docsAdded++
			}
		}
	}

	PutRowBuffer(keyBuf)

	if len(addRows) > 0 {
		addRowsAll = append(addRowsAll, addRows)
	}

	return dictionaryDeltas, docsAdded, docsDeleted,
		addRowsAll, updateRowsAll, deleteRowsAll, segDirtiness
}
