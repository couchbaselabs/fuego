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
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

func (udc *Fuego) Open() error {
	// acquire the write mutex for the duration of Open()
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open the kv store
	storeConstructor := registry.KVStoreConstructorByName(udc.storeName)
	if storeConstructor == nil {
		return index.ErrorUnknownStorageType
	}

	// now open the store
	var err error
	udc.store, err = storeConstructor(&mergeOperator, udc.storeConfig)
	if err != nil {
		return err
	}

	// start a reader to look at the index
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return err
	}

	value, err := kvreader.Get(VersionKey)
	if err != nil {
		_ = kvreader.Close()
		return err
	}

	if value != nil {
		err = udc.loadStoreLOCKED(kvreader)
		if err != nil {
			_ = kvreader.Close()
			return err
		}

		// set doc count
		udc.m.Lock()
		udc.docCount, err = udc.countDocs(kvreader)
		udc.m.Unlock()

		err = kvreader.Close()
	} else {
		// new index, close the reader and open writer to init
		err = kvreader.Close()
		if err != nil {
			return err
		}

		var kvwriter store.KVWriter
		kvwriter, err = udc.store.Writer()
		if err != nil {
			return err
		}
		defer func() {
			if cerr := kvwriter.Close(); err == nil && cerr != nil {
				err = cerr
			}
		}()

		// init the index
		err = udc.initStoreLOCKED(kvwriter)
	}

	return err
}

func (udc *Fuego) initStoreLOCKED(kvwriter store.KVWriter) error {
	rowsAll := [][]KVRow{
		{
			NewVersionRow(udc.version),
			udc.summaryRow,
		},
	}

	return udc.batchRows(kvwriter, nil, rowsAll, nil)
}

func (udc *Fuego) loadStoreLOCKED(kvreader store.KVReader) (err error) {
	var val []byte
	val, err = kvreader.Get(VersionKey)
	if err != nil {
		return
	}

	var vr *VersionRow
	vr, err = NewVersionRowKV(VersionKey, val)
	if err != nil {
		return
	}
	if vr.version != Version {
		err = IncompatibleVersion
		return
	}

	// load summary row
	val, err = kvreader.Get(SummaryKey)
	if err != nil {
		return
	}

	var sr *SummaryRow
	sr, err = NewSummaryRowKV(SummaryKey, val)
	if err != nil {
		return
	}

	udc.summaryRow = sr

	// load field rows
	it := kvreader.PrefixIterator([]byte{'f'})
	defer func() {
		if cerr := it.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	k, v, valid := it.Current()
	for valid {
		var fieldRow *FieldRow
		fieldRow, err = NewFieldRowKV(k, v)
		if err != nil {
			return
		}

		udc.fieldCache.AddExisting(fieldRow.name, fieldRow.index)

		it.Next()
		k, v, valid = it.Current()
	}

	return
}

func (udc *Fuego) countDocs(kvreader store.KVReader) (count uint64, err error) {
	it := kvreader.PrefixIterator([]byte{'b'})
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

func (udc *Fuego) Close() error {
	return udc.store.Close()
}
