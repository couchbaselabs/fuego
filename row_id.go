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

func (i *IndexReader) ExternalID(id index.IndexInternalID) (string, error) {
	if useUpsideDownApproach {
		return string(id), nil
	}

	buf := GetRowBuffer()
	if cap(buf) < IdRowKeySize {
		buf = make([]byte, IdRowKeySize)
	}
	keyBuf := buf[:IdRowKeySize]

	keyBuf[0] = 'I'
	used := copy(keyBuf[1:], id)

	docIDBytes, err := i.kvreader.Get(keyBuf[:1+used])

	PutRowBuffer(buf)

	if err != nil {
		return "", err
	}

	return string(docIDBytes), nil
}

func (i *IndexReader) InternalID(docID string) (index.IndexInternalID, error) {
	if useUpsideDownApproach {
		return index.IndexInternalID(docID), nil
	}

	docIDBytes := []byte(docID)

	backIndexRow, err := backIndexRowForDocID(i.kvreader, docIDBytes, nil)
	if err != nil {
		return nil, err
	}

	rv := make([]byte, 16)

	binary.LittleEndian.PutUint64(rv[:8], backIndexRow.segId)
	binary.LittleEndian.PutUint64(rv[8:], backIndexRow.recId)

	return index.IndexInternalID(rv), nil
}

// ------------------------------------------------------

func IdRowKeyPrefix(segId uint64, buf []byte) int {
	buf[0] = 'I'
	binary.LittleEndian.PutUint64(buf[1:], segId)
	return 9
}

var IdRowKeySize = 1 + 8 + 8

// ------------------------------------------------------

type IdRow struct {
	segId uint64
	recId uint64
	docID []byte
}

func (p *IdRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *IdRow) KeySize() int {
	return IdRowKeySize
}

func (p *IdRow) KeyTo(buf []byte) (int, error) {
	used := IdRowKeyPrefix(p.segId, buf)
	binary.LittleEndian.PutUint64(buf[used:], p.recId)
	return used + 8, nil
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
