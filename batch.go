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

func (udc *Fuego) Batch(batch *index.Batch) (err error) {
	analysisStart := time.Now()

	analyzeResultCh := make(chan *AnalyzeAuxResult, len(batch.IndexOps))

	var numUpdates uint64
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
				AnalyzeAuxQueue <- &AnalyzeAuxReq{
					Index:         udc,
					Doc:           doc,
					WantBackIndex: true,
					ResultCh:      analyzeResultCh,
				}
			}
		}
	}()

	// retrieve back index rows concurrent with analysis
	docBackIndexRowErr := error(nil)
	docBackIndexRowCh := make(chan *docBackIndexRow, len(batch.IndexOps))

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// The segId's decrease or drop downwards from MAX_UINT64.
	udc.summaryRow.LastUsedSegId = udc.summaryRow.LastUsedSegId - 1

	go func() {
		defer close(docBackIndexRowCh)

		// open a reader for backindex lookup
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

	// wait for analysis result
	analyzeResults := map[string]*AnalyzeAuxResult{}
	var itemsDeQueued uint64
	for itemsDeQueued < numUpdates {
		analyzeResult := <-analyzeResultCh
		analyzeResults[analyzeResult.DocID] = analyzeResult
		itemsDeQueued++
	}
	close(analyzeResultCh)

	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	docsAdded := uint64(0)
	docsDeleted := uint64(0)

	indexStart := time.Now()

	// prepare a list of rows
	var addRowsAll [][]KVRow
	var updateRowsAll [][]KVRow
	var deleteRowsAll [][]KVRow

	// add the internal ops
	var updateRows []KVRow
	var deleteRows []KVRow

	for internalKey, internalValue := range batch.InternalOps {
		if internalValue == nil {
			// delete
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

	// process back index rows as they arrive
	lastSegRecId := SegRecId{
		SegId: udc.summaryRow.LastUsedSegId,
		RecId: 0,
	}

	for dbir := range docBackIndexRowCh {
		if dbir.doc == nil && dbir.backIndexRow != nil {
			// delete
			deleteRows := udc.deleteSingle(dbir.docIDBytes, dbir.backIndexRow, nil)
			if len(deleteRows) > 0 {
				deleteRowsAll = append(deleteRowsAll, deleteRows)
			}
			docsDeleted++
		} else if dbir.doc != nil {
			lastSegRecId.RecId++

			addRows, updateRows, deleteRows :=
				udc.mergeOldAndNew(&lastSegRecId, dbir.backIndexRow, analyzeResults[dbir.docID])
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
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}

	err = udc.batchRows(kvwriter, addRowsAll, updateRowsAll, deleteRowsAll)
	if err != nil {
		_ = kvwriter.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = kvwriter.Close()

	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))

	if err == nil {
		udc.m.Lock()
		udc.docCount += docsAdded
		udc.docCount -= docsDeleted
		udc.m.Unlock()
		atomic.AddUint64(&udc.stats.updates, numUpdates)
		atomic.AddUint64(&udc.stats.deletes, docsDeleted)
		atomic.AddUint64(&udc.stats.batches, 1)
		atomic.AddUint64(&udc.stats.numPlainTextBytesIndexed, numPlainTextBytes)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
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

func (udc *Fuego) mergeOldAndNew(segRecId *SegRecId, backIndexRow *BackIndexRow, ar *AnalyzeAuxResult) (
	addRows []KVRow, updateRows []KVRow, deleteRows []KVRow) {
	numRows := len(ar.FieldRows) + len(ar.TermFreqRows) + len(ar.StoredRows)

	numRows += len(ar.StoredRows) // For SegRecStoredRows.

	if ar.BackIndexRow != nil {
		numRows += 1
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

		for _, row := range ar.StoredRows {
			addRows = append(addRows, row.ToSegRecStoredRow(segRecId))
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

				updateRows = append(updateRows, row.ToSegRecStoredRow(segRecId))

				continue
			}
		}

		addRows = append(addRows, row)

		addRows = append(addRows, row.ToSegRecStoredRow(segRecId))
	}

	updateRows = append(updateRows, ar.BackIndexRow)

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
