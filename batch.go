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

//go:generate protoc --gofast_out=. fuego.proto

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
	doc          *document.Document // If deletion, doc will be nil.
	backIndexRow *BackIndexRow
}

func (udc *Fuego) Batch(batch *index.Batch) (err error) {
	analysisStart := time.Now()

	resultChan := make(chan *index.AnalysisResult, len(batch.IndexOps))

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
				aw := index.NewAnalysisWork(udc, doc, resultChan)
				// put the work on the queue
				udc.analysisQueue.Queue(aw)
			}
		}
	}()

	// retrieve back index rows concurrent with analysis
	docBackIndexRowErr := error(nil)
	docBackIndexRowCh := make(chan *docBackIndexRow, len(batch.IndexOps))

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	go func() {
		defer close(docBackIndexRowCh)

		// open a reader for backindex lookup
		var kvreader store.KVReader
		kvreader, err = udc.store.Reader()
		if err != nil {
			docBackIndexRowErr = err
			return
		}

		for docID, doc := range batch.IndexOps {
			backIndexRow, err := backIndexRowForDoc(kvreader, index.IndexInternalID(docID))
			if err != nil {
				docBackIndexRowErr = err
				return
			}

			docBackIndexRowCh <- &docBackIndexRow{docID, doc, backIndexRow}
		}

		err = kvreader.Close()
		if err != nil {
			docBackIndexRowErr = err
			return
		}
	}()

	// wait for analysis result
	newRowsMap := make(map[string][]index.IndexRow)
	var itemsDeQueued uint64
	for itemsDeQueued < numUpdates {
		result := <-resultChan
		newRowsMap[result.DocID] = result.Rows
		itemsDeQueued++
	}
	close(resultChan)

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
	for dbir := range docBackIndexRowCh {
		if dbir.doc == nil && dbir.backIndexRow != nil {
			// delete
			deleteRows := udc.deleteSingle(dbir.docID, dbir.backIndexRow, nil)
			if len(deleteRows) > 0 {
				deleteRowsAll = append(deleteRowsAll, deleteRows)
			}
			docsDeleted++
		} else if dbir.doc != nil {
			addRows, updateRows, deleteRows := udc.mergeOldAndNew(dbir.backIndexRow, newRowsMap[dbir.docID])
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
		if udc.docCountMax < udc.docCount {
			udc.docCountMax = udc.docCount
		}
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

func (udc *Fuego) mergeOldAndNew(backIndexRow *BackIndexRow, rows []index.IndexRow) (
	addRows []KVRow, updateRows []KVRow, deleteRows []KVRow) {
	addRows = make([]KVRow, 0, len(rows))
	updateRows = make([]KVRow, 0, len(rows))
	deleteRows = make([]KVRow, 0, len(rows))

	existingTermKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllTermKeys() {
		existingTermKeys[string(key)] = true
	}

	existingStoredKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllStoredKeys() {
		existingStoredKeys[string(key)] = true
	}

	keyBuf := GetRowBuffer()
	for _, row := range rows {
		switch row := row.(type) {
		case *TermFrequencyRow:
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}
			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingTermKeys[string(keyBuf[:keySize])]; ok {
				updateRows = append(updateRows, row)
				delete(existingTermKeys, string(keyBuf[:keySize]))
			} else {
				addRows = append(addRows, row)
			}
		case *StoredRow:
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}
			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingStoredKeys[string(keyBuf[:keySize])]; ok {
				updateRows = append(updateRows, row)
				delete(existingStoredKeys, string(keyBuf[:keySize]))
			} else {
				addRows = append(addRows, row)
			}
		default:
			updateRows = append(updateRows, row)
		}
	}
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
