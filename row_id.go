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

type IdRow struct {
	segId uint64
	recId uint64
	docID []byte
}

func (p *IdRow) SegId() uint64 {
	return p.segId
}

func (p *IdRow) RecId() uint64 {
	return p.recId
}

func (p *IdRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *IdRow) KeySize() int {
	return 1 + 8 + 8
}

func (p *IdRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'I'
	binary.LittleEndian.PutUint64(buf[1:], p.segId)
	binary.LittleEndian.PutUint64(buf[1+8:], p.recId)
	return 1 + 8 + 8, nil
}

func (p *IdRow) Value() []byte {
	buf := make([]byte, p.ValueSize())
	size, _ := p.ValueTo(buf)
	return buf[:size]
}

func (p *IdRow) ValueSize() int {
	return len(p.docID)
}

func (p *IdRow) ValueTo(buf []byte) (int, error) {
	return copy(buf, p.docID), nil
}

func (p *IdRow) String() string {
	return fmt.Sprintf("Id segId: %d, recId: %d, docID: %s",
		p.segId, p.recId, string(p.docID))
}

func NewIdRow(segId uint64, recId uint64, docID []byte) *IdRow {
	return &IdRow{
		segId: segId,
		recId: recId,
		docID: docID,
	}
}

func NewIdRowK(key []byte) (*IdRow, error) {
	rv := &IdRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (p *IdRow) parseK(key []byte) error {
	p.segId = binary.LittleEndian.Uint64(key[1 : 1+8])
	p.recId = binary.LittleEndian.Uint64(key[1+8 : 1+8+8])
	return nil
}

func (p *IdRow) parseV(value []byte) error {
	p.docID = append(p.docID[:0], value...)
	return nil
}

func NewIdRowKV(key, value []byte) (*IdRow, error) {
	rv, err := NewIdRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseV(value)
	if err != nil {
		return nil, err
	}

	return rv, nil
}
