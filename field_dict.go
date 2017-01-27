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
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type FieldDict struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	dictRow     *DictionaryRow
	dictEntry   *index.DictEntry
	field       uint16
}

func newFieldDict(indexReader *IndexReader, field uint16,
	startTerm, endTerm []byte) (*FieldDict, error) {
	startKey := NewDictionaryRow(startTerm, field, 0).Key()

	if endTerm == nil {
		endTerm = []byte{ByteSeparator}
	} else {
		endTerm = incrementBytes(endTerm)
	}

	endKey := NewDictionaryRow(endTerm, field, 0).Key()

	it := indexReader.kvreader.RangeIterator(startKey, endKey)

	return &FieldDict{
		indexReader: indexReader,
		iterator:    it,
		dictRow:     &DictionaryRow{},   // Pre-alloced, reused row.
		dictEntry:   &index.DictEntry{}, // Pre-alloced, reused entry.
		field:       field,
	}, nil
}

func (r *FieldDict) Next() (*index.DictEntry, error) {
	key, val, valid := r.iterator.Current()
	if !valid {
		return nil, nil
	}

	err := r.dictRow.parseDictionaryK(key)
	if err != nil {
		return nil, fmt.Errorf("error parsing dictionary row key: %v", err)
	}

	err = r.dictRow.parseDictionaryV(val)
	if err != nil {
		return nil, fmt.Errorf("error parsing dictionary row val: %v", err)
	}

	r.dictEntry.Term = string(r.dictRow.term)
	r.dictEntry.Count = r.dictRow.count

	r.iterator.Next()

	return r.dictEntry, nil

}

func (r *FieldDict) Close() error {
	return r.iterator.Close()
}

// -------------------------------------------------

// The returned fieldIds are already sorted ASC.
func (udc *Fuego) LoadFieldIds(kvreader store.KVReader) (fieldIds uint16s, err error) {
	// TODO: Replace the FieldRow iterator by instead accessing the
	// index.FieldCache, which doesn't currently have a public API to
	// grab all the fieldId's.
	if kvreader == nil {
		kvreader, err = udc.store.Reader()
		if err != nil {
			return nil, err
		}
		defer kvreader.Close()
	}

	fieldRow := FieldRow{}

	it := kvreader.PrefixIterator(FieldRowPrefix)

	k, v, valid := it.Current()
	for valid {
		err := fieldRow.ParseKV(k, v)
		if err != nil {
			it.Close()
			return nil, err
		}

		fieldIds = append(fieldIds, fieldRow.index)

		it.Next()
		k, v, valid = it.Current()
	}

	it.Close()

	sort.Sort(fieldIds)

	return fieldIds, nil
}
