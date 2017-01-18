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
	"bytes"
	"encoding/binary"
	"math"
	"sort"
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

type batchEntries []*batchEntry

func (a batchEntries) Len() int {
	return len(a)
}

func (a batchEntries) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a batchEntries) Less(i, j int) bool {
	return bytes.Compare(a[i].analyzeResult.DocIDBytes, a[j].analyzeResult.DocIDBytes) < 0
}

// --------------------------------------------------

type batchEntryTFR struct {
	batchEntry     *batchEntry
	termFreqRowIdx int // Index into batchEntry.analyzeResult.TermFreqRows array.
}

// --------------------------------------------------

func (udc *Fuego) Batch(batch *index.Batch) error {
	analysisStart := time.Now()

	analyzeResultCh := make(chan *AnalyzeAuxResult, len(batch.IndexOps))

	var numUpdates int
	var numPlainTextBytes uint64

	for _, doc := range batch.IndexOps {
		if doc != nil {
			numUpdates++
			numPlainTextBytes += doc.NumPlainTextBytes()
		}
	}

	go func() {
		for _, doc := range batch.IndexOps {
			if doc != nil {
				// TODO: Change WantBackIndex to false when 100% fuego.
				AnalyzeAuxQueue <- &AnalyzeAuxReq{
					Index:         udc,
					Doc:           doc,
					WantBackIndex: true,
					ResultCh:      analyzeResultCh,
				}
			}
		}
	}()

	docBackIndexRowErr := error(nil)
	docBackIndexRowCh := make(chan *docBackIndexRow, len(batch.IndexOps))

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// The segId's decrease or drop downwards from MAX_UINT64,
	// which allows newer/younger seg's to appear first in iterators.
	udc.summaryRow.LastUsedSegId = udc.summaryRow.LastUsedSegId - 1

	go func() { // Retrieve back index rows concurrent with analysis.
		defer close(docBackIndexRowCh)

		kvreader, err := udc.store.Reader()
		if err != nil {
			docBackIndexRowErr = err
			return
		}
		defer kvreader.Close()

		for docID, doc := range batch.IndexOps {
			docIDBytes := []byte(docID)

			backIndexRow, err := backIndexRowForDoc(kvreader, docIDBytes)
			if err != nil {
				docBackIndexRowErr = err
				return
			}

			docBackIndexRowCh <- &docBackIndexRow{docID, docIDBytes, doc, backIndexRow}
		}
	}()

	// TODO: Retrieve docIDRow's concurrent with analysis.
	// TODO: Or, just put the recId's also in the backIndexRow's.

	// Wait for analyze results.
	batchEntriesPre := make([]batchEntry, len(batch.IndexOps))           // Prealloc'ed.
	batchEntriesArr := make(batchEntries, 0, len(batch.IndexOps))        // Sorted by docID.
	batchEntriesMap := make(map[string]*batchEntry, len(batch.IndexOps)) // Keyed by docID.

	var numBatchEntries int

	for numBatchEntries < numUpdates {
		analyzeResult := <-analyzeResultCh

		batchEntry := &batchEntriesPre[numBatchEntries]
		batchEntry.analyzeResult = analyzeResult

		batchEntriesArr = append(batchEntriesArr, batchEntry)

		batchEntriesMap[analyzeResult.DocID] = batchEntry

		numBatchEntries++
	}

	close(analyzeResultCh)

	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	indexStart := time.Now()

	sort.Sort(batchEntriesArr) // Sort batchEntriesArr by docID ASC.

	// Assign recId's, starting from 1, based on the position of each
	// docID in the sorted docID's.
	var nextRecId uint64 = 1
	var numTermFreqRows int

	for _, batchEntry := range batchEntriesArr {
		batchEntry.recId = nextRecId
		nextRecId++

		numTermFreqRows += len(batchEntry.analyzeResult.TermFreqRows)
	}

	// Fill the fieldTerms array and the fieldTermBatchEntryTFRs map.
	fieldTerms := fieldTerms(nil)
	fieldTermBatchEntryTFRs := map[fieldTerm][]*batchEntryTFR{}

	batchEntryTFRPre := make([]batchEntryTFR, numTermFreqRows) // Prealloc'ed.
	batchEntryTFRUsed := 0

	for _, batchEntry := range batchEntriesArr {
		for tfrIdx, tfr := range batchEntry.analyzeResult.TermFreqRows {
			fieldTerm := fieldTerm{tfr.field, string(tfr.term)}

			batchEntryTFR := &batchEntryTFRPre[batchEntryTFRUsed]
			batchEntryTFRUsed += 1

			batchEntryTFR.batchEntry = batchEntry
			batchEntryTFR.termFreqRowIdx = tfrIdx

			// Since we are driving the loop from the sorted
			// batchEntriesArr, the array values in the
			// fieldTermBatchEntryTFRs will inherit the docID / recId
			// ASC ordering.  In a sense, we're bucketing or collating
			// or grouping by the fieldTerm's, keeping the overall
			// batchEntriesArr ordering.
			batchEntryTFRs := fieldTermBatchEntryTFRs[fieldTerm]
			if batchEntryTFRs == nil {
				fieldTerms = append(fieldTerms, fieldTerm)
			}

			fieldTermBatchEntryTFRs[fieldTerm] =
				append(batchEntryTFRs, batchEntryTFR)
		}
	}

	// Sort the fieldTerms by field ASC, term ASC.
	sort.Sort(fieldTerms)

	var addRowsAll [][]KVRow
	var updateRowsAll [][]KVRow
	var deleteRowsAll [][]KVRow

	// Add the postings.
	for _, fieldTerm := range fieldTerms { // Sorted by field, term ASC.
		batchEntryTFRs := fieldTermBatchEntryTFRs[fieldTerm]

		recIds := make([]uint64, len(batchEntryTFRs))
		freqNorms := make([]uint32, 2*len(batchEntryTFRs))
		vectors := make([][]*TermVector, len(batchEntryTFRs))

		for i, batchEntryTFR := range batchEntryTFRs {
			batchEntry := batchEntryTFR.batchEntry

			recIds[i] = batchEntry.recId

			tfr := batchEntry.analyzeResult.TermFreqRows[batchEntryTFR.termFreqRowIdx]

			j := i * 2
			freqNorms[j] = uint32(tfr.freq)
			freqNorms[j+1] = math.Float32bits(tfr.norm)

			vectors[i] = tfr.vectors
		}
	}

	// Add the internal ops.
	if len(batch.InternalOps) > 0 {
		var updateRows []KVRow
		var deleteRows []KVRow

		for internalKey, internalValue := range batch.InternalOps {
			if internalValue == nil {
				deleteInternalRow := NewInternalRow([]byte(internalKey), nil)
				deleteRows = append(deleteRows, deleteInternalRow)
			} else {
				updateInternalRow := NewInternalRow([]byte(internalKey), internalValue)
				updateRows = append(updateRows, updateInternalRow)
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
	docsAdded := uint64(0)
	docsDeleted := uint64(0)

	for dbir := range docBackIndexRowCh {
		if dbir.doc == nil {
			if dbir.backIndexRow != nil { // A deletion.
				deleteRows := udc.deleteSingle(dbir.docIDBytes, dbir.backIndexRow, nil)
				if len(deleteRows) > 0 {
					deleteRowsAll = append(deleteRowsAll, deleteRows)
				}
				docsDeleted++
			}
		} else {
			addRows, updateRows, deleteRows := udc.mergeOldAndNew(
				udc.summaryRow.LastUsedSegId, dbir.backIndexRow, batchEntriesMap[dbir.docID])
			if len(addRows) > 0 {
				addRowsAll = append(addRowsAll, addRows)
			}
			if len(updateRows) > 0 {
				updateRowsAll = append(updateRowsAll, updateRows)
			}
			if len(deleteRows) > 0 {
				deleteRowsAll = append(deleteRowsAll, deleteRows)
			}
			if dbir.backIndexRow == nil {
				docsAdded++
			}
		}
	}

	if docBackIndexRowErr != nil {
		return docBackIndexRowErr
	}

	// start a writer for this batch
	kvwriter, err := udc.store.Writer()
	if err != nil {
		return err
	}

	err = udc.batchRows(kvwriter, addRowsAll, updateRowsAll, deleteRowsAll)

	cerr := kvwriter.Close()
	if cerr != nil && err == nil {
		err = cerr
	}

	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))

	if err != nil {
		atomic.AddUint64(&udc.stats.errors, 1)
		return err
	}

	udc.m.Lock()
	udc.docCount += docsAdded
	udc.docCount -= docsDeleted
	udc.m.Unlock()

	atomic.AddUint64(&udc.stats.updates, uint64(numUpdates))
	atomic.AddUint64(&udc.stats.deletes, docsDeleted)
	atomic.AddUint64(&udc.stats.batches, 1)
	atomic.AddUint64(&udc.stats.numPlainTextBytesIndexed, numPlainTextBytes)

	return nil
}

func (udc *Fuego) deleteSingle(idBytes []byte, backIndexRow *BackIndexRow, deleteRows []KVRow) []KVRow {
	for _, backIndexEntry := range backIndexRow.termEntries {
		tfr := NewTermFrequencyRow([]byte(*backIndexEntry.Term), uint16(*backIndexEntry.Field), idBytes, 0, 0)
		deleteRows = append(deleteRows, tfr)
	}

	for _, se := range backIndexRow.storedEntries {
		sf := NewStoredRow(idBytes, uint16(*se.Field), se.ArrayPositions, 'x', nil)
		deleteRows = append(deleteRows, sf)
	}

	// also delete the backIndexRow itself
	return append(deleteRows, backIndexRow)
}

func (udc *Fuego) batchRows(writer store.KVWriter,
	addRowsAll [][]KVRow, updateRowsAll [][]KVRow, deleteRowsAll [][]KVRow) (err error) {
	dictionaryDeltas := make(map[string]int64)

	// count up bytes needed for buffering.
	addNum := 0
	addKeyBytes := 0
	addValBytes := 0

	updateNum := 0
	updateKeyBytes := 0
	updateValBytes := 0

	deleteNum := 0
	deleteKeyBytes := 0

	rowBuf := GetRowBuffer()

	for _, addRows := range addRowsAll {
		for _, row := range addRows {
			tfr, ok := row.(*TermFrequencyRow)
			if ok {
				if tfr.DictionaryRowKeySize() > len(rowBuf) {
					rowBuf = make([]byte, tfr.DictionaryRowKeySize())
				}
				dictKeySize, err := tfr.DictionaryRowKeyTo(rowBuf)
				if err != nil {
					return err
				}
				dictionaryDeltas[string(rowBuf[:dictKeySize])] += 1
			}
			addKeyBytes += row.KeySize()
			addValBytes += row.ValueSize()
		}
		addNum += len(addRows)
	}

	for _, updateRows := range updateRowsAll {
		for _, row := range updateRows {
			updateKeyBytes += row.KeySize()
			updateValBytes += row.ValueSize()
		}
		updateNum += len(updateRows)
	}

	for _, deleteRows := range deleteRowsAll {
		for _, row := range deleteRows {
			tfr, ok := row.(*TermFrequencyRow)
			if ok {
				// need to decrement counter
				if tfr.DictionaryRowKeySize() > len(rowBuf) {
					rowBuf = make([]byte, tfr.DictionaryRowKeySize())
				}
				dictKeySize, err := tfr.DictionaryRowKeyTo(rowBuf)
				if err != nil {
					return err
				}
				dictionaryDeltas[string(rowBuf[:dictKeySize])] -= 1
			}
			deleteKeyBytes += row.KeySize()
		}
		deleteNum += len(deleteRows)
	}

	PutRowBuffer(rowBuf)

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

func (udc *Fuego) mergeOldAndNew(segId uint64, backIndexRow *BackIndexRow, batchEntry *batchEntry) (
	addRows []KVRow, updateRows []KVRow, deleteRows []KVRow) {
	ar := batchEntry.analyzeResult

	docIDRow := NewDocIDRow(ar.DocIDBytes, segId, batchEntry.recId)

	numRows := len(ar.FieldRows) + len(ar.TermFreqRows) + len(ar.StoredRows)

	numRows += len(ar.StoredRows) // For SegRecStoredRow's.

	if ar.BackIndexRow != nil {
		numRows += 1

		numRows += 1 // For docIDRow.
	}

	addRows = make([]KVRow, 0, numRows)

	if backIndexRow == nil {
		for _, row := range ar.FieldRows {
			addRows = append(addRows, row)
		}
		for _, row := range ar.TermFreqRows {
			addRows = append(addRows, row)
		}
		for _, row := range ar.StoredRows {
			addRows = append(addRows, row)
		}

		if ar.BackIndexRow != nil {
			addRows = append(addRows, ar.BackIndexRow)
		}

		// fuego rows...

		addRows = append(addRows, docIDRow)

		for _, row := range ar.StoredRows {
			addRows = append(addRows, row.ToSegRecStoredRow(segId, batchEntry.recId))
		}

		return addRows, nil, nil
	}

	updateRows = make([]KVRow, 0, numRows)
	deleteRows = make([]KVRow, 0, numRows)

	var mark struct{}

	var existingTermKeys map[string]struct{}
	backIndexTermKeys := backIndexRow.AllTermKeys()
	if len(backIndexTermKeys) > 0 {
		existingTermKeys = make(map[string]struct{}, len(backIndexTermKeys))
		for _, key := range backIndexTermKeys {
			existingTermKeys[string(key)] = mark
		}
	}

	var existingStoredKeys map[string]struct{}
	backIndexStoredKeys := backIndexRow.AllStoredKeys()
	if len(backIndexStoredKeys) > 0 {
		existingStoredKeys = make(map[string]struct{}, len(backIndexStoredKeys))
		for _, key := range backIndexStoredKeys {
			existingStoredKeys[string(key)] = mark
		}
	}

	keyBuf := GetRowBuffer()

	for _, row := range ar.FieldRows {
		updateRows = append(updateRows, row)
	}

	for _, row := range ar.TermFreqRows {
		if existingTermKeys != nil {
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}

			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingTermKeys[string(keyBuf[:keySize])]; ok {
				updateRows = append(updateRows, row)
				delete(existingTermKeys, string(keyBuf[:keySize]))

				continue
			}
		}

		addRows = append(addRows, row)
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

				updateRows = append(updateRows,
					row.ToSegRecStoredRow(segId, batchEntry.recId))

				continue
			}
		}

		addRows = append(addRows, row)

		addRows = append(addRows,
			row.ToSegRecStoredRow(segId, batchEntry.recId))
	}

	if ar.BackIndexRow != nil {
		updateRows = append(updateRows, ar.BackIndexRow)
	}

	updateRows = append(updateRows, docIDRow)

	PutRowBuffer(keyBuf)

	// any of the existing rows that weren't updated need to be deleted
	for existingTermKey := range existingTermKeys {
		termFreqRow, err := NewTermFrequencyRowK([]byte(existingTermKey))
		if err == nil {
			deleteRows = append(deleteRows, termFreqRow)
		}
	}

	// any of the existing stored fields that weren't updated need to be deleted
	for existingStoredKey := range existingStoredKeys {
		storedRow, err := NewStoredRowK([]byte(existingStoredKey))
		if err == nil {
			deleteRows = append(deleteRows, storedRow)
		}
	}

	return addRows, updateRows, deleteRows
}
