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
	"bytes"
	"encoding/binary"
	"fmt"
)

type StoredRow struct {
	doc            []byte
	field          uint16
	arrayPositions []uint64
	typ            byte
	value          []byte
}

func (s *StoredRow) Key() []byte {
	buf := make([]byte, s.KeySize())
	size, _ := s.KeyTo(buf)
	return buf[0:size]
}

func (s *StoredRow) KeySize() int {
	return 1 + len(s.doc) + 1 + 2 + (binary.MaxVarintLen64 * len(s.arrayPositions))
}

func (s *StoredRow) KeyTo(buf []byte) (int, error) {
	docLen := len(s.doc)
	buf[0] = 's'
	copy(buf[1:], s.doc)
	buf[1+docLen] = ByteSeparator
	binary.LittleEndian.PutUint16(buf[1+docLen+1:], s.field)
	bytesUsed := 1 + docLen + 1 + 2
	for _, arrayPosition := range s.arrayPositions {
		varbytes := binary.PutUvarint(buf[bytesUsed:], arrayPosition)
		bytesUsed += varbytes
	}
	return bytesUsed, nil
}

func (s *StoredRow) Value() []byte {
	buf := make([]byte, s.ValueSize())
	size, _ := s.ValueTo(buf)
	return buf[:size]
}

func (s *StoredRow) ValueSize() int {
	return len(s.value) + 1
}

func (s *StoredRow) ValueTo(buf []byte) (int, error) {
	buf[0] = s.typ
	used := copy(buf[1:], s.value)
	return used + 1, nil
}

func (s *StoredRow) String() string {
	return fmt.Sprintf("Document: %s Field %d, Array Positions: %v, Type: %s Value: %s",
		s.doc, s.field, s.arrayPositions, string(s.typ), s.value)
}

func (s *StoredRow) ScanPrefixForDoc() []byte {
	docLen := len(s.doc)
	buf := make([]byte, 1+docLen+1)
	buf[0] = 's'
	copy(buf[1:], s.doc)
	buf[1+docLen] = ByteSeparator
	return buf
}

func NewStoredRow(docID []byte, field uint16, arrayPositions []uint64, typ byte, value []byte) *StoredRow {
	return &StoredRow{
		doc:            docID,
		field:          field,
		arrayPositions: arrayPositions,
		typ:            typ,
		value:          value,
	}
}

func NewStoredRowK(key []byte) (*StoredRow, error) {
	rv := StoredRow{}

	buf := bytes.NewBuffer(key)
	_, err := buf.ReadByte() // type
	if err != nil {
		return nil, err
	}

	rv.doc, err = buf.ReadBytes(ByteSeparator)
	if len(rv.doc) < 2 { // 1 for min doc id length, 1 for separator
		err = fmt.Errorf("invalid doc length 0")
		return nil, err
	}

	rv.doc = rv.doc[:len(rv.doc)-1] // trim off separator byte

	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

	rv.arrayPositions = make([]uint64, 0)
	nextArrayPos, err := binary.ReadUvarint(buf)
	for err == nil {
		rv.arrayPositions = append(rv.arrayPositions, nextArrayPos)
		nextArrayPos, err = binary.ReadUvarint(buf)
	}

	return &rv, nil
}

func NewStoredRowKV(key, value []byte) (*StoredRow, error) {
	rv, err := NewStoredRowK(key)
	if err != nil {
		return nil, err
	}

	rv.typ = value[0]
	rv.value = value[1:]

	return rv, nil
}
