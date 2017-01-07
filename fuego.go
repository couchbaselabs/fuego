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
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type Fuego struct {
	version       uint8
	path          string
	storeName     string
	storeConfig   map[string]interface{}
	store         store.KVStore
	fieldCache    *index.FieldCache
	analysisQueue *index.AnalysisQueue
	stats         *indexStat

	m sync.RWMutex
	// fields protected by m
	docCount    uint64
	docCountMax uint64 // Only grows - max docCount seen.

	writeMutex sync.Mutex
}

func NewFuego(storeName string, storeConfig map[string]interface{},
	analysisQueue *index.AnalysisQueue) (index.Index, error) {
	rv := &Fuego{
		version:       Version,
		fieldCache:    index.NewFieldCache(),
		storeName:     storeName,
		storeConfig:   storeConfig,
		analysisQueue: analysisQueue,
	}
	rv.stats = &indexStat{i: rv}
	return rv, nil
}

func (udc *Fuego) Reader() (index.IndexReader, error) {
	kvr, err := udc.store.Reader()
	if err != nil {
		return nil, fmt.Errorf("error opening store reader: %v", err)
	}
	udc.m.RLock()
	defer udc.m.RUnlock()
	return &IndexReader{
		index:       udc,
		kvreader:    kvr,
		docCount:    udc.docCount,
		docCountMax: udc.docCountMax,
	}, nil
}

func (udc *Fuego) Stats() json.Marshaler {
	return udc.stats
}

func (udc *Fuego) StatsMap() map[string]interface{} {
	return udc.stats.statsMap()
}

func (udc *Fuego) Advanced() (store.KVStore, error) {
	return udc.store, nil
}

func (udc *Fuego) Update(doc *document.Document) (err error) {
	// do analysis before acquiring write lock
	analysisStart := time.Now()
	numPlainTextBytes := doc.NumPlainTextBytes()
	resultChan := make(chan *index.AnalysisResult)
	aw := index.NewAnalysisWork(udc, doc, resultChan)

	// put the work on the queue
	udc.analysisQueue.Queue(aw)

	// wait for the result
	result := <-resultChan
	close(resultChan)
	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open a reader for backindex lookup
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = backIndexRowForDoc(kvreader, index.IndexInternalID(doc.ID))
	if err != nil {
		_ = kvreader.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = kvreader.Close()
	if err != nil {
		return
	}

	// start a writer for this update
	indexStart := time.Now()
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	// prepare a list of rows
	var addRowsAll [][]KVRow
	var updateRowsAll [][]KVRow
	var deleteRowsAll [][]KVRow

	addRows, updateRows, deleteRows := udc.mergeOldAndNew(backIndexRow, result.Rows)
	if len(addRows) > 0 {
		addRowsAll = append(addRowsAll, addRows)
	}
	if len(updateRows) > 0 {
		updateRowsAll = append(updateRowsAll, updateRows)
	}
	if len(deleteRows) > 0 {
		deleteRowsAll = append(deleteRowsAll, deleteRows)
	}

	err = udc.batchRows(kvwriter, addRowsAll, updateRowsAll, deleteRowsAll)
	if err == nil && backIndexRow == nil {
		udc.m.Lock()
		udc.docCount++
		if udc.docCountMax < udc.docCount {
			udc.docCountMax = udc.docCount
		}
		udc.m.Unlock()
	}
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		atomic.AddUint64(&udc.stats.updates, 1)
		atomic.AddUint64(&udc.stats.numPlainTextBytesIndexed, numPlainTextBytes)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *Fuego) Delete(id string) (err error) {
	indexStart := time.Now()

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open a reader for backindex lookup
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = backIndexRowForDoc(kvreader, index.IndexInternalID(id))
	if err != nil {
		_ = kvreader.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = kvreader.Close()
	if err != nil {
		return
	}

	if backIndexRow == nil {
		atomic.AddUint64(&udc.stats.deletes, 1)
		return
	}

	// start a writer for this delete
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	var deleteRowsAll [][]KVRow

	deleteRows := udc.deleteSingle(id, backIndexRow, nil)
	if len(deleteRows) > 0 {
		deleteRowsAll = append(deleteRowsAll, deleteRows)
	}

	err = udc.batchRows(kvwriter, nil, nil, deleteRowsAll)
	if err == nil {
		udc.m.Lock()
		udc.docCount--
		udc.m.Unlock()
	}
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		atomic.AddUint64(&udc.stats.deletes, 1)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *Fuego) deleteSingle(id string, backIndexRow *BackIndexRow, deleteRows []KVRow) []KVRow {
	idBytes := []byte(id)

	for _, backIndexEntry := range backIndexRow.termEntries {
		tfr := NewTermFrequencyRow([]byte(*backIndexEntry.Term), uint16(*backIndexEntry.Field), idBytes, 0, 0)
		deleteRows = append(deleteRows, tfr)
	}
	for _, se := range backIndexRow.storedEntries {
		sf := NewStoredRow(idBytes, uint16(*se.Field), se.ArrayPositions, 'x', nil)
		deleteRows = append(deleteRows, sf)
	}

	// also delete the back entry itself
	deleteRows = append(deleteRows, backIndexRow)
	return deleteRows
}

func (udc *Fuego) SetInternal(key, val []byte) (err error) {
	internalRow := NewInternalRow(key, val)
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()
	var writer store.KVWriter
	writer, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := writer.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	batch := writer.NewBatch()
	batch.Set(internalRow.Key(), internalRow.Value())

	return writer.ExecuteBatch(batch)
}

func (udc *Fuego) DeleteInternal(key []byte) (err error) {
	internalRow := NewInternalRow(key, nil)
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()
	var writer store.KVWriter
	writer, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := writer.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	batch := writer.NewBatch()
	batch.Delete(internalRow.Key())
	return writer.ExecuteBatch(batch)
}

func (udc *Fuego) rowCount() (count uint64, err error) {
	// start an isolated reader for use during the row count
	kvreader, err := udc.store.Reader()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvreader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	it := kvreader.RangeIterator(nil, nil)
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	_, _, valid := it.Current()
	for valid {
		count++
		it.Next()
		_, _, valid = it.Current()
	}

	return
}
