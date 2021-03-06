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
	"math"
)

type TermVector struct {
	field          uint16
	arrayPositions []uint64
	pos            uint64
	start          uint64
	end            uint64
}

func (tv *TermVector) String() string {
	return fmt.Sprintf("Field: %d Pos: %d Start: %d End %d ArrayPositions: %#v",
		tv.field, tv.pos, tv.start, tv.end, tv.arrayPositions)
}

type TermFrequencyRow struct {
	term    []byte
	doc     []byte
	freq    uint64
	vectors []*TermVector
	norm    float32
	field   uint16
}

func (tfr *TermFrequencyRow) Term() []byte {
	return tfr.term
}

func (tfr *TermFrequencyRow) Freq() uint64 {
	return tfr.freq
}

func (tfr *TermFrequencyRow) ScanPrefixForField() []byte {
	buf := make([]byte, 3)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	return buf
}

func (tfr *TermFrequencyRow) ScanPrefixForFieldTermPrefix() []byte {
	buf := make([]byte, 3+len(tfr.term))
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	copy(buf[3:], tfr.term)
	return buf
}

func (tfr *TermFrequencyRow) ScanPrefixForFieldTerm() []byte {
	buf := make([]byte, 3+len(tfr.term)+1)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = ByteSeparator
	return buf
}

func (tfr *TermFrequencyRow) Key() []byte {
	buf := make([]byte, tfr.KeySize())
	size, _ := tfr.KeyTo(buf)
	return buf[:size]
}

func (tfr *TermFrequencyRow) KeySize() int {
	return 3 + len(tfr.term) + 1 + len(tfr.doc)
}

func (tfr *TermFrequencyRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = ByteSeparator
	docLen := copy(buf[3+termLen+1:], tfr.doc)
	return 3 + termLen + 1 + docLen, nil
}

func (tfr *TermFrequencyRow) KeyAppendTo(buf []byte) ([]byte, error) {
	keySize := tfr.KeySize()
	if cap(buf) < keySize {
		buf = make([]byte, keySize)
	}
	actualSize, err := tfr.KeyTo(buf[0:keySize])
	return buf[0:actualSize], err
}

func (tfr *TermFrequencyRow) DictionaryRowKey() []byte {
	return NewDictionaryRow(tfr.term, tfr.field, 0).Key()
}

func (tfr *TermFrequencyRow) DictionaryRowKeySize() int {
	return dictionaryRowKeySize(tfr.field, tfr.term)
}

func (tfr *TermFrequencyRow) DictionaryRowKeyTo(buf []byte) (int, error) {
	return dictionaryRowKeyTo(tfr.field, tfr.term, buf)
}

func (tfr *TermFrequencyRow) Value() []byte {
	buf := make([]byte, tfr.ValueSize())
	size, _ := tfr.ValueTo(buf)
	return buf[:size]
}

func (tfr *TermFrequencyRow) ValueSize() int {
	bufLen := binary.MaxVarintLen64 + binary.MaxVarintLen64
	for _, vector := range tfr.vectors {
		bufLen += (binary.MaxVarintLen64 * 4) + (1+len(vector.arrayPositions))*binary.MaxVarintLen64
	}
	return bufLen
}

func (tfr *TermFrequencyRow) ValueTo(buf []byte) (int, error) {
	used := binary.PutUvarint(buf[:binary.MaxVarintLen64], tfr.freq)

	normuint32 := math.Float32bits(tfr.norm)
	newbuf := buf[used : used+binary.MaxVarintLen64]
	used += binary.PutUvarint(newbuf, uint64(normuint32))

	for _, vector := range tfr.vectors {
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], uint64(vector.field))
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.pos)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.start)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.end)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], uint64(len(vector.arrayPositions)))
		for _, arrayPosition := range vector.arrayPositions {
			used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], arrayPosition)
		}
	}
	return used, nil
}

func (tfr *TermFrequencyRow) String() string {
	return fmt.Sprintf("Term: `%s` Field: %d DocID: `%s` Frequency: %d Norm: %f Vectors: %v",
		string(tfr.term), tfr.field, string(tfr.doc), tfr.freq, tfr.norm, tfr.vectors)
}

func InitTermFrequencyRow(tfr *TermFrequencyRow, term []byte, field uint16,
	docID []byte, freq uint64, norm float32) *TermFrequencyRow {
	tfr.term = term
	tfr.field = field
	tfr.doc = docID
	tfr.freq = freq
	tfr.norm = norm
	return tfr
}

func NewTermFrequencyRow(term []byte, field uint16, docID []byte, freq uint64, norm float32) *TermFrequencyRow {
	return &TermFrequencyRow{
		term:  term,
		field: field,
		doc:   docID,
		freq:  freq,
		norm:  norm,
	}
}

func NewTermFrequencyRowWithTermVectors(term []byte, field uint16,
	docID []byte, freq uint64, norm float32, vectors []*TermVector) *TermFrequencyRow {
	return &TermFrequencyRow{
		term:    term,
		field:   field,
		doc:     docID,
		freq:    freq,
		norm:    norm,
		vectors: vectors,
	}
}

func NewTermFrequencyRowK(key []byte) (*TermFrequencyRow, error) {
	rv := &TermFrequencyRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (tfr *TermFrequencyRow) parseK(key []byte) error {
	keyLen := len(key)
	if keyLen < 3 {
		return fmt.Errorf("invalid term frequency key, no valid field")
	}
	tfr.field = binary.LittleEndian.Uint16(key[1:3])

	termEndPos := bytes.IndexByte(key[3:], ByteSeparator)
	if termEndPos < 0 {
		return fmt.Errorf("invalid term frequency key, no byte separator terminating term")
	}
	tfr.term = key[3 : 3+termEndPos]

	docLen := keyLen - (3 + termEndPos + 1)
	if docLen < 1 {
		return fmt.Errorf("invalid term frequency key, empty docid")
	}
	tfr.doc = key[3+termEndPos+1:]

	return nil
}

func (tfr *TermFrequencyRow) parseKDoc(key []byte, term []byte) error {
	tfr.doc = key[3+len(term)+1:]
	if len(tfr.doc) <= 0 {
		return fmt.Errorf("invalid term frequency key, empty docid")
	}

	return nil
}

func (tfr *TermFrequencyRow) parseV(value []byte, includeTermVectors bool) error {
	var bytesRead int
	tfr.freq, bytesRead = binary.Uvarint(value)
	if bytesRead <= 0 {
		return fmt.Errorf("invalid term frequency value, invalid frequency")
	}
	currOffset := bytesRead

	var norm uint64
	norm, bytesRead = binary.Uvarint(value[currOffset:])
	if bytesRead <= 0 {
		return fmt.Errorf("invalid term frequency value, no norm")
	}
	currOffset += bytesRead

	tfr.norm = math.Float32frombits(uint32(norm))

	tfr.vectors = nil
	if !includeTermVectors {
		return nil
	}

	var field uint64
	field, bytesRead = binary.Uvarint(value[currOffset:])
	for bytesRead > 0 {
		currOffset += bytesRead
		tv := TermVector{}
		tv.field = uint16(field)
		// at this point we expect at least one term vector
		if tfr.vectors == nil {
			tfr.vectors = make([]*TermVector, 0)
		}

		tv.pos, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no position")
		}
		currOffset += bytesRead

		tv.start, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no start")
		}
		currOffset += bytesRead

		tv.end, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no end")
		}
		currOffset += bytesRead

		var arrayPositionsLen uint64 = 0
		arrayPositionsLen, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no arrayPositionLen")
		}
		currOffset += bytesRead

		if arrayPositionsLen > 0 {
			tv.arrayPositions = make([]uint64, arrayPositionsLen)
			for i := 0; uint64(i) < arrayPositionsLen; i++ {
				tv.arrayPositions[i], bytesRead = binary.Uvarint(value[currOffset:])
				if bytesRead <= 0 {
					return fmt.Errorf("invalid term frequency value, vector contains no arrayPosition of index %d", i)
				}
				currOffset += bytesRead
			}
		}

		tfr.vectors = append(tfr.vectors, &tv)
		// try to read next record (may not exist)
		field, bytesRead = binary.Uvarint(value[currOffset:])
	}
	if len(value[currOffset:]) > 0 && bytesRead <= 0 {
		return fmt.Errorf("invalid term frequency value, vector field invalid")
	}

	return nil
}

func NewTermFrequencyRowKV(key, value []byte) (*TermFrequencyRow, error) {
	rv, err := NewTermFrequencyRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseV(value, true)
	if err != nil {
		return nil, err
	}
	return rv, nil
}
