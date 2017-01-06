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
)

func backIndexRowForDoc(kvreader store.KVReader, docID index.IndexInternalID) (*BackIndexRow, error) {
	// use a temporary row structure to build key
	tempRow := &BackIndexRow{
		doc: docID,
	}

	keyBuf := GetRowBuffer()
	if tempRow.KeySize() > len(keyBuf) {
		keyBuf = make([]byte, 2*tempRow.KeySize())
	}
	defer PutRowBuffer(keyBuf)
	keySize, err := tempRow.KeyTo(keyBuf)
	if err != nil {
		return nil, err
	}

	value, err := kvreader.Get(keyBuf[:keySize])
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	backIndexRow, err := NewBackIndexRowKV(keyBuf[:keySize], value)
	if err != nil {
		return nil, err
	}
	return backIndexRow, nil
}
