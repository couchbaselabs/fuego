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

func (udc *Fuego) Open() (err error) {
	// acquire the write mutex for the duratin of Open()
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open the kv store
	storeConstructor := registry.KVStoreConstructorByName(udc.storeName)
	if storeConstructor == nil {
		err = index.ErrorUnknownStorageType
		return
	}

	// now open the store
	udc.store, err = storeConstructor(&mergeOperator, udc.storeConfig)
	if err != nil {
		return
	}

	// start a reader to look at the index
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	var value []byte
	value, err = kvreader.Get(VersionKey)
	if err != nil {
		_ = kvreader.Close()
		return
	}

	if value != nil {
		err = udc.loadSchema(kvreader)
		if err != nil {
			_ = kvreader.Close()
			return
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
			return
		}

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

		// init the index
		err = udc.init(kvwriter)
	}

	return
}

func (udc *Fuego) init(kvwriter store.KVWriter) (err error) {
	// version marker
	rowsAll := [][]KVRow{
		{NewVersionRow(udc.version)},
	}

	err = udc.batchRows(kvwriter, nil, rowsAll, nil)
	return
}

func (udc *Fuego) loadSchema(kvreader store.KVReader) (err error) {
	it := kvreader.PrefixIterator([]byte{'f'})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	key, val, valid := it.Current()
	for valid {
		var fieldRow *FieldRow
		fieldRow, err = NewFieldRowKV(key, val)
		if err != nil {
			return
		}
		udc.fieldCache.AddExisting(fieldRow.name, fieldRow.index)

		it.Next()
		key, val, valid = it.Current()
	}

	val, err = kvreader.Get([]byte{'v'})
	if err != nil {
		return
	}
	var vr *VersionRow
	vr, err = NewVersionRowKV([]byte{'v'}, val)
	if err != nil {
		return
	}
	if vr.version != Version {
		err = IncompatibleVersion
		return
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
