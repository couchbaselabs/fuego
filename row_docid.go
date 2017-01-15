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

type DocIDRow struct {
	docID []byte
	segId uint64
	recId uint64
}

func (s *DocIDRow) Key() []byte {
	buf := make([]byte, s.KeySize())
	size, _ := s.KeyTo(buf)
	return buf[:size]
}

func (s *DocIDRow) KeySize() int {
	return 1 + len(s.docID) + 1 + 8
}

func (s *DocIDRow) KeyTo(buf []byte) (int, error) {
	docLen := len(s.docID)
	buf[0] = 'D'
	copy(buf[1:], s.docID)
	buf[1+docLen] = ByteSeparator
	binary.LittleEndian.PutUint64(buf[1+docLen+1:], s.segId)
	return 1 + docLen + 1 + 8, nil
}

func (s *DocIDRow) Value() []byte {
	buf := make([]byte, s.ValueSize())
	size, _ := s.ValueTo(buf)
	return buf[:size]
}

func (s *DocIDRow) ValueSize() int {
	return 8
}

func (s *DocIDRow) ValueTo(buf []byte) (int, error) {
	binary.LittleEndian.PutUint64(buf, s.recId)
	return 8, nil
}

func (s *DocIDRow) String() string {
	return fmt.Sprintf("Document: %s, segId: %d, recId: %d",
		s.docID, s.segId, s.recId)
}

func NewDocIDRow(docID []byte, segId uint64, recId uint64) *DocIDRow {
	return &DocIDRow{
		docID: docID,
		segId: segId,
		recId: recId,
	}
}

func NewDocIDRowK(key []byte) (*DocIDRow, error) {
	return &DocIDRow{
		docID: key[1 : len(key)-8-1],
		segId: binary.LittleEndian.Uint64(key[len(key)-8:]),
	}, nil
}

func NewDocIDRowKV(key, value []byte) (*DocIDRow, error) {
	rv, err := NewDocIDRowK(key)
	if err != nil {
		return nil, err
	}

	rv.recId = binary.LittleEndian.Uint64(value)

	return rv, nil
}
