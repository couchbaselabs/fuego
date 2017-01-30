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

	"github.com/blevesearch/bleve/index"
)

type PostingVecsRow struct {
	field uint16
	term  []byte
	segId uint64

	// The encoded has the rec's encoded as...
	// - numRecs (uint32). // TODO: This should be uint64?
	// - recOffsets, an array of numRecs # of uint32's,
	//     which are zero-based indexes into the recParts array
	//     where each rec starts.
	// - recParts, an array of uint32's that represent the actual rec's,
	//     where each rec looks like...
	//     - numVecs (uint32).
	//     - vecOffsets, an array of numVecs # of uint32's,
	//         which are zero-based indexes into the vecParts array
	//         where each vec starts.
	//     - vecParts, an array of uint32's that represent the actual vec's,
	//         where each vec looks like...
	//         - field  (uint16),
	//         - length (uint16) where length = end - start,
	//         - start  (uint32),
	//         - pos    (uint32),
	//         - and zero or more array positions (uint32's),
	//             where the number of array positions is determined
	//             by the next offset.
	encoded []uint32

	// TODO: Get rid of the 0th' offset, which is always 0?
}

// Returns the TermFieldVectors for the i'th record in this PostingVecsRow.
func (p *PostingVecsRow) TermFieldVectors(i int,
	fieldCache *index.FieldCache, prealloc []*index.TermFieldVector) (
	[]*index.TermFieldVector, error) {
	rec, err := p.TermFieldVectorsEncoded(i)
	if err != nil || rec == nil {
		return nil, err
	}

	numVecs := int(rec[0])
	vecOffsets := rec[1 : 1+numVecs]
	vecParts := rec[1+numVecs:]

	rv := prealloc
	if cap(rv) < numVecs {
		rva := make([]index.TermFieldVector, numVecs)
		rv = make([]*index.TermFieldVector, numVecs)
		for j := 0; j < numVecs; j++ {
			rv[j] = &rva[j]
		}
	}
	rv = rv[:numVecs]

	var field uint16
	var fieldName string

	for j := 0; j < numVecs; j++ {
		tv := rv[j]

		vecOffset := vecOffsets[j]
		vec := vecParts[vecOffset:]
		if j+1 < numVecs {
			vecLen := vecOffsets[j+1] - vecOffset
			vec = vec[:vecLen]
		}

		fieldAndLength := vec[0]

		if field == 0 {
			field = uint16(0x0000ffff & (fieldAndLength >> 16))
			fieldName = fieldCache.FieldIndexed(field)
		}

		tv.Field = fieldName
		tv.Start = uint64(vec[1])
		tv.End = tv.Start + uint64(0x0000ffff&fieldAndLength)
		tv.Pos = uint64(vec[2])

		arrayPositions := vec[3:]
		if cap(tv.ArrayPositions) < len(arrayPositions) {
			tv.ArrayPositions = make([]uint64, len(arrayPositions))
		}
		tv.ArrayPositions = tv.ArrayPositions[:len(arrayPositions)]
		for k, pos := range arrayPositions {
			tv.ArrayPositions[k] = uint64(pos)
		}
	}

	return rv, nil
}

func (p *PostingVecsRow) TermFieldVectorsEncoded(i int) ([]uint32, error) {
	if len(p.encoded) <= 0 {
		return nil, nil
	}

	numRecs := int(p.encoded[0])
	recOffset := int(p.encoded[1+i])

	rec := p.encoded[1+numRecs+recOffset:]
	if i+1 < numRecs {
		recLen := int(p.encoded[1+i+1]) - recOffset
		rec = rec[:recLen]
	}

	return rec, nil
}

func (p *PostingVecsRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *PostingVecsRow) KeySize() int {
	return PostingRowKeySize(p.term)
}

func (p *PostingVecsRow) KeyTo(buf []byte) (int, error) {
	used := PostingRowKeyPrefix(p.field, p.term, buf)
	binary.BigEndian.PutUint64(buf[used:used+8], p.segId)
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
	if len(p.encoded) <= 0 {
		return 0, nil
	}

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

	return fmt.Sprintf("Field: %d, Term: `%q`, numVecs: %d, len(encoded): %d",
		p.field, string(p.term), numVecs, len(p.encoded))
}

func NewPostingVecsRow(field uint16, term []byte, segId uint64, encoded []uint32) *PostingVecsRow {
	return &PostingVecsRow{
		field:   field,
		term:    term,
		segId:   segId,
		encoded: encoded,
	}
}

func NewPostingVecsRowFromVectors(field uint16, term []byte, segId uint64,
	vectors [][]*TermVector) *PostingVecsRow {
	numRecs := len(vectors)
	numVecs := 0
	numArrayPositions := 0
	for _, recTermVectors := range vectors {
		numVecs += len(recTermVectors)
		for _, termVector := range recTermVectors {
			numArrayPositions += len(termVector.arrayPositions)
		}
	}

	if numVecs <= 0 {
		return NewPostingVecsRow(field, term, segId, nil)
	}

	size := 1 + numRecs + numRecs + numVecs + (numVecs * 3) + numArrayPositions

	encoded := make([]uint32, size)
	encoded[0] = uint32(numRecs)

	recOffsets := encoded[1 : 1+numRecs]

	recParts := encoded[1+numVecs:]
	recPartsUsed := 0

	for i, recTermVectors := range vectors {
		recOffsets[i] = uint32(recPartsUsed)

		numVecs := len(recTermVectors)
		recParts[recPartsUsed] = uint32(numVecs)
		recPartsUsed++

		vecOffsets := recParts[recPartsUsed : recPartsUsed+numVecs]
		recPartsUsed += numVecs

		vecParts := recParts[recPartsUsed:]
		vecPartsUsed := 0

		for j, termVector := range recTermVectors {
			vecOffsets[j] = uint32(vecPartsUsed)

			fieldAndLength :=
				(uint32(0xffff0000) & (uint32(termVector.field) << 16)) |
					(uint32(0x0000ffff) & uint32(termVector.end-termVector.start))
			vecParts[vecPartsUsed] = fieldAndLength
			vecPartsUsed++

			vecParts[vecPartsUsed] = uint32(termVector.start)
			vecPartsUsed++

			vecParts[vecPartsUsed] = uint32(termVector.pos)
			vecPartsUsed++

			for _, arrayPosition := range termVector.arrayPositions {
				vecParts[vecPartsUsed] = uint32(arrayPosition)
				vecPartsUsed++
			}
		}

		recPartsUsed += vecPartsUsed
	}

	return NewPostingVecsRow(field, term, segId, encoded)
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
	if keyLen < 13 {
		return fmt.Errorf("invalid PostingVecsRow key")
	}
	if key[keyLen-1] != 'v' {
		return fmt.Errorf("invalid PostingVecsRow key suffix: %s",
			string(key))
	}
	p.field = binary.LittleEndian.Uint16(key[1:3])
	p.term = key[3 : keyLen-10]
	p.segId = binary.BigEndian.Uint64(key[keyLen-9 : keyLen-1])
	return nil
}

func (p *PostingVecsRow) parseV(value []byte) (err error) {
	var encoded []uint32

	if len(value) > 0 {
		encoded, err = ByteSliceToUint32Slice(value)
		if err != nil {
			return err
		}
	}

	p.encoded = encoded

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
