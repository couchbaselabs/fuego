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
	"math"
	"sync"

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

	m sync.RWMutex // Protects the fields that follow.

	summaryRow *SummaryRow

	docCount uint64

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
		summaryRow:    NewSummaryRow(math.MaxUint64),
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
	rv := &IndexReader{
		index:    udc,
		kvreader: kvr,
		docCount: udc.docCount,
	}
	udc.m.RUnlock()

	return rv, nil
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

func (udc *Fuego) Update(doc *document.Document) error {
	b := index.NewBatch()
	b.Update(doc)
	return udc.Batch(b)
}

func (udc *Fuego) Delete(id string) error {
	b := index.NewBatch()
	b.Delete(id)
	return udc.Batch(b)
}

func (udc *Fuego) rowCount() (uint64, error) {
	// start an isolated reader for use during the row count
	kvreader, err := udc.store.Reader()
	if err != nil {
		return 0, err
	}
	defer kvreader.Close()

	it := kvreader.RangeIterator(nil, nil)
	defer it.Close()

	var count uint64

	_, _, valid := it.Current()
	for valid {
		count++
		it.Next()
		_, _, valid = it.Current()
	}

	return count, nil
}
