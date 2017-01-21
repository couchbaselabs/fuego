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
	"math"
)

type PostingFreqNormsRow struct {
	field     uint16 // Part of the row's key.
	term      []byte // Part of the row's key.
	segId     uint64 // Part of the row's key.
	freqNorms []uint32
}

func (p *PostingFreqNormsRow) Freq(i int) uint32 {
	return p.freqNorms[i*2]
}

func (p *PostingFreqNormsRow) Norm(i int) float32 {
	return math.Float32frombits(p.freqNorms[(i*2)+1])
}

func (p *PostingFreqNormsRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *PostingFreqNormsRow) KeySize() int {
	return PostingRowKeySize(p.term)
}

func (p *PostingFreqNormsRow) KeyTo(buf []byte) (int, error) {
	used := PostingRowKeyPrefix(p.field, p.term, buf)
	binary.LittleEndian.PutUint64(buf[used:used+8], p.segId)
	used += 8
	buf[used] = 'f'
	used += 1
	return used, nil
}

func (p *PostingFreqNormsRow) Value() []byte {
	buf := make([]byte, p.ValueSize())
	size, _ := p.ValueTo(buf)
	return buf[:size]
}

func (p *PostingFreqNormsRow) ValueSize() int {
	return len(p.freqNorms) * 4
}

func (p *PostingFreqNormsRow) ValueTo(buf []byte) (int, error) {
	bufRecIds, err := Uint32SliceToByteSlice(p.freqNorms)
	if err != nil {
		return 0, err
	}
	return copy(buf, bufRecIds), nil
}

func (p *PostingFreqNormsRow) String() string {
	return fmt.Sprintf("Field: %d, Term: `%s`, len(freqNorms): %d",
		p.field, string(p.term), len(p.freqNorms))
}

func NewPostingFreqNormsRow(field uint16, term []byte, segId uint64, freqNorms []uint32) *PostingFreqNormsRow {
	return &PostingFreqNormsRow{
		field:     field,
		term:      term,
		segId:     segId,
		freqNorms: freqNorms,
	}
}

func NewPostingFreqNormsRowK(key []byte) (*PostingFreqNormsRow, error) {
	rv := &PostingFreqNormsRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (p *PostingFreqNormsRow) parseK(key []byte) error {
	keyLen := len(key)
	if keyLen < 13 {
		return fmt.Errorf("invalid PostingFreqNormsRow key")
	}
	if key[keyLen-1] != 'f' {
		return fmt.Errorf("invalid PostingFreqNormsRow key suffix: %s",
			string(key))
	}
	p.field = binary.LittleEndian.Uint16(key[1:3])
	p.term = append(p.term[:0], key[3:len(key)-10]...)
	p.segId = binary.LittleEndian.Uint64(key[len(key)-9 : len(key)-1])
	return nil
}

func (p *PostingFreqNormsRow) parseV(value []byte) error {
	freqNorms, err := ByteSliceToUint32Slice(value)
	if err != nil {
		return err
	}
	p.freqNorms = append(p.freqNorms[:0], freqNorms...)
	return nil
}

func NewPostingFreqNormsRowKV(key, value []byte) (*PostingFreqNormsRow, error) {
	rv, err := NewPostingFreqNormsRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseV(value)
	if err != nil {
		return nil, err
	}

	return rv, nil
}
