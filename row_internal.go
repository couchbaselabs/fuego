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

type InternalRow struct {
	key []byte
	val []byte
}

func (i *InternalRow) Key() []byte {
	buf := make([]byte, i.KeySize())
	size, _ := i.KeyTo(buf)
	return buf[:size]
}

func (i *InternalRow) KeySize() int {
	return len(i.key) + 1
}

func (i *InternalRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'i'
	actual := copy(buf[1:], i.key)
	return 1 + actual, nil
}

func (i *InternalRow) Value() []byte {
	return i.val
}

func (i *InternalRow) ValueSize() int {
	return len(i.val)
}

func (i *InternalRow) ValueTo(buf []byte) (int, error) {
	actual := copy(buf, i.val)
	return actual, nil
}

func (i *InternalRow) String() string {
	return fmt.Sprintf("InternalStore - Key: %s (% x) Val: %s (% x)", i.key, i.key, i.val, i.val)
}

func NewInternalRow(key, val []byte) *InternalRow {
	return &InternalRow{
		key: key,
		val: val,
	}
}

func NewInternalRowKV(key, value []byte) (*InternalRow, error) {
	rv := InternalRow{}
	rv.key = key[1:]
	rv.val = value
	return &rv, nil
}
