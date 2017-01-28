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
)

const ByteSeparator byte = 0xff

type KVRowStream chan KVRow

type KVRow interface {
	KeySize() int
	KeyTo([]byte) (int, error)
	Key() []byte
	Value() []byte
	ValueSize() int
	ValueTo([]byte) (int, error)
}

func ParseFromKeyValue(key, value []byte) (KVRow, error) {
	if len(key) > 0 {
		switch key[0] {
		case 'P':
			switch key[len(key)-1] {
			case 'c':
				return NewPostingRecIdsRowKV(key, value)
			case 'f':
				return NewPostingFreqNormsRowKV(key, value)
			case 'v':
				return NewPostingVecsRowKV(key, value)
			}
			return nil, fmt.Errorf("Unknown posting type '%s'", string(key[len(key)-1]))
		case 'I':
			return NewIdRowKV(key, value)
		case 'b':
			return NewBackIndexRowKV(key, value)
		case 'd':
			return NewDictionaryRowKV(key, value)
		case 'f':
			return NewFieldRowKV(key, value)
		case 'i':
			return NewInternalRowKV(key, value)
		case 's':
			return NewStoredRowKV(key, value)
		case 't':
			return NewTermFrequencyRowKV(key, value)
		case 'v':
			return NewVersionRowKV(key, value)
		case 'x':
			return NewDeletionRowKV(key, value)
		}
		return nil, fmt.Errorf("Unknown row type '%s'", string(key[0]))
	}
	return nil, fmt.Errorf("Invalid empty key")
}
