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

func PostingRowKeySize(term []byte) int {
	return 1 + 2 + len(term) + 1 + 8 + 1
}

func PostingRowKeyPrefix(field uint16, term []byte, buf []byte) int {
	buf[0] = 'P'
	binary.LittleEndian.PutUint16(buf[1:3], field)
	used := 3 + copy(buf[3:], term)
	buf[used] = ByteSeparator
	used += 1
	return used
}

// --------------------------------------------------

type PostingRecIdsRow struct {
	field  uint16 // Part of the row's key.
	term   []byte // Part of the row's key.
	segId  uint64 // Part of the row's key.
	recIds []uint64
}

func (p *PostingRecIdsRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *PostingRecIdsRow) KeySize() int {
	return PostingRowKeySize(p.term)
}

func (p *PostingRecIdsRow) KeyTo(buf []byte) (int, error) {
	used := PostingRowKeyPrefix(p.field, p.term, buf)
	binary.LittleEndian.PutUint64(buf[used:used+8], p.segId)
	used += 8
	buf[used] = 'c' // Suffix 'c' comes lexically before 'f' (from freqNorm rows).
	used += 1
	return used, nil
}

func (p *PostingRecIdsRow) DictionaryRowKeySize() int {
	return dictionaryRowKeySize(p.field, p.term)
}

func (p *PostingRecIdsRow) DictionaryRowKeyTo(buf []byte) (int, error) {
	return dictionaryRowKeyTo(p.field, p.term, buf)
}

func (p *PostingRecIdsRow) Value() []byte {
	buf := make([]byte, p.ValueSize())
	size, _ := p.ValueTo(buf)
	return buf[:size]
}

func (p *PostingRecIdsRow) ValueSize() int {
	return len(p.recIds) * 8
}

func (p *PostingRecIdsRow) ValueTo(buf []byte) (int, error) {
	bufRecIds, err := Uint64SliceToByteSlice(p.recIds)
	if err != nil {
		return 0, err
	}
	return copy(buf, bufRecIds), nil
}

func (p *PostingRecIdsRow) String() string {
	return fmt.Sprintf("Field: %d, Term: `%s`, len(recIds): %d",
		p.field, string(p.term), len(p.recIds))
}

func NewPostingRecIdsRow(field uint16, term []byte, segId uint64, recIds []uint64) *PostingRecIdsRow {
	return &PostingRecIdsRow{
		field:  field,
		term:   term,
		segId:  segId,
		recIds: recIds,
	}
}

func NewPostingRecIdsRowK(key []byte) (*PostingRecIdsRow, error) {
	rv := &PostingRecIdsRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (p *PostingRecIdsRow) parseK(key []byte) error {
	keyLen := len(key)
	if keyLen < 13 {
		return fmt.Errorf("invalid PostingRecIdsRow key")
	}
	if key[keyLen-1] != 'c' {
		return fmt.Errorf("invalid PostingRecIdsRow key suffix: %s",
			string(key))
	}
	p.field = binary.LittleEndian.Uint16(key[1:3])
	p.term = key[3 : len(key)-10]
	p.segId = binary.LittleEndian.Uint64(key[len(key)-9 : len(key)-1])
	return nil
}

func (p *PostingRecIdsRow) parseV(value []byte) error {
	recIds, err := ByteSliceToUint64Slice(value)
	if err != nil {
		return err
	}
	p.recIds = recIds
	return nil
}

func NewPostingRecIdsRowKV(key, value []byte) (*PostingRecIdsRow, error) {
	rv, err := NewPostingRecIdsRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseV(value)
	if err != nil {
		return nil, err
	}

	return rv, nil
}
