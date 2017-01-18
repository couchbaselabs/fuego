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

type PostingVecsRow struct {
	field uint16
	term  []byte
	segId uint64

	// The encoded has the vec's encoded as...
	// - numVecs (uint32).
	// - offsets, an array of numVecs # of uint32's,
	//     which are zero-based indexes into the parts array
	//     where each vec starts.
	// - parts, an array of uint32's that represent the actual vec's,
	//     where each vec is a...
	//     - field  (uint16),
	//     - length (uint16) where length = end - start,
	//     - start  (uint32),
	//     - pos    (uint32),
	//     - and zero or more array positions (uint32's),
	//         where the number of array positions is determined
	//         by the next offset.
	encoded []uint32

	// TODO: Get rid of the 0th' offset, which is always 0?
}

func (p *PostingVecsRow) Field() uint16 {
	return p.field
}

func (p *PostingVecsRow) Term() []byte {
	return p.term
}

func (p *PostingVecsRow) TermVector(i int, prealloc *TermVector) (
	*TermVector, error) {
	numVecs := p.encoded[0]
	offset := p.encoded[1+i]
	parts := p.encoded[1+numVecs+offset:]
	if i+1 < int(numVecs) {
		partsLen := p.encoded[1+i+1] - offset
		parts = parts[:partsLen]
	}

	rv := prealloc
	if rv == nil {
		rv = &TermVector{}
	}

	fieldAndLength := parts[0]

	rv.field = uint16(0x0000ffff & (fieldAndLength >> 16))
	rv.start = uint64(parts[1])
	rv.end = rv.start + uint64(0x0000ffff&fieldAndLength)
	rv.pos = uint64(parts[2])

	arrayPositions := parts[3:] // TODO: Maybe avoid array copy?
	rv.arrayPositions = rv.arrayPositions[:0]
	for _, pos := range arrayPositions {
		rv.arrayPositions = append(rv.arrayPositions, uint64(pos))
	}

	return rv, nil
}

func (p *PostingVecsRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *PostingVecsRow) KeySize() int {
	return 1 + 2 + len(p.term) + 8 + 1
}

func (p *PostingVecsRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'P'
	binary.LittleEndian.PutUint16(buf[1:3], p.field)
	used := 3 + copy(buf[3:], p.term)
	binary.LittleEndian.PutUint64(buf[used:used+8], p.segId)
	used += 8
	buf[used] = 'v'
	used += 1
	return used, nil
}

func (p *PostingVecsRow) Value() []byte {
	buf := make([]byte, p.ValueSize())
	size, _ := p.ValueTo(buf)
	return buf[:size]
}

func (p *PostingVecsRow) ValueSize() int {
	return len(p.encoded) * 4
}

func (p *PostingVecsRow) ValueTo(buf []byte) (int, error) {
	bufEncoded, err := Uint32SliceToByteSlice(p.encoded)
	if err != nil {
		return 0, err
	}
	return copy(buf, bufEncoded), nil
}

func (p *PostingVecsRow) String() string {
	var numVecs uint32
	if len(p.encoded) > 0 {
		numVecs = p.encoded[0]
	}

	return fmt.Sprintf("Field: %d, Term: `%s`, numVecs: %d, len(encoded): %d",
		p.field, string(p.term), numVecs, len(p.encoded))
}

func NewPostingVecsRow(field uint16, term []byte, encoded []uint32) *PostingVecsRow {
	return &PostingVecsRow{
		field:   field,
		term:    term,
		encoded: encoded,
	}
}

func NewPostingVecsRowK(key []byte) (*PostingVecsRow, error) {
	rv := &PostingVecsRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (p *PostingVecsRow) parseK(key []byte) error {
	keyLen := len(key)
	if keyLen < 4 {
		return fmt.Errorf("invalid PostingVecsRow key")
	}
	p.field = binary.LittleEndian.Uint16(key[1:3])
	p.term = append(p.term[:0], key[3:len(key)-9]...)
	p.segId = binary.LittleEndian.Uint64(key[len(key)-9 : len(key)-1])
	return nil
}

func (p *PostingVecsRow) parseV(value []byte) error {
	encoded, err := ByteSliceToUint32Slice(value)
	if err != nil {
		return err
	}
	p.encoded = append(p.encoded[:0], encoded...)
	return nil
}

func NewPostingVecsRowKV(key, value []byte) (*PostingVecsRow, error) {
	rv, err := NewPostingVecsRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseV(value)
	if err != nil {
		return nil, err
	}

	return rv, nil
}
