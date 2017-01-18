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
	"fmt"
)

const DictionaryRowMaxValueSize = binary.MaxVarintLen64

type DictionaryRow struct {
	term  []byte
	count uint64
	field uint16
}

func (dr *DictionaryRow) Key() []byte {
	buf := make([]byte, dr.KeySize())
	size, _ := dr.KeyTo(buf)
	return buf[:size]
}

func (dr *DictionaryRow) KeySize() int {
	return dictionaryRowKeySize(dr.field, dr.term)
}

func dictionaryRowKeySize(field uint16, term []byte) int {
	return len(term) + 3
}

func (dr *DictionaryRow) KeyTo(buf []byte) (int, error) {
	return dictionaryRowKeyTo(dr.field, dr.term, buf)
}

func dictionaryRowKeyTo(field uint16, term, buf []byte) (int, error) {
	buf[0] = 'd'
	binary.LittleEndian.PutUint16(buf[1:3], field)
	size := copy(buf[3:], term)
	return size + 3, nil
}

func (dr *DictionaryRow) Value() []byte {
	buf := make([]byte, dr.ValueSize())
	size, _ := dr.ValueTo(buf)
	return buf[:size]
}

func (dr *DictionaryRow) ValueSize() int {
	return DictionaryRowMaxValueSize
}

func (dr *DictionaryRow) ValueTo(buf []byte) (int, error) {
	used := binary.PutUvarint(buf, dr.count)
	return used, nil
}

func (dr *DictionaryRow) String() string {
	return fmt.Sprintf("Dictionary Term: `%s` Field: %d Count: %d ", string(dr.term), dr.field, dr.count)
}

func NewDictionaryRow(term []byte, field uint16, count uint64) *DictionaryRow {
	return &DictionaryRow{
		term:  term,
		field: field,
		count: count,
	}
}

func NewDictionaryRowKV(key, value []byte) (*DictionaryRow, error) {
	rv, err := NewDictionaryRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseDictionaryV(value)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func NewDictionaryRowK(key []byte) (*DictionaryRow, error) {
	rv := &DictionaryRow{}
	err := rv.parseDictionaryK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (dr *DictionaryRow) parseDictionaryK(key []byte) error {
	dr.field = binary.LittleEndian.Uint16(key[1:3])
	if dr.term != nil {
		dr.term = dr.term[:0]
	}
	dr.term = append(dr.term, key[3:]...)
	return nil
}

func (dr *DictionaryRow) parseDictionaryV(value []byte) error {
	count, nread := binary.Uvarint(value)
	if nread <= 0 {
		return fmt.Errorf("DictionaryRow parse Uvarint error, nread: %d", nread)
	}
	dr.count = count
	return nil
}
