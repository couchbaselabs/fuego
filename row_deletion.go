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

func DeletionRowKeyPrefix(segId uint64, buf []byte) int {
	buf[0] = 'x'
	binary.BigEndian.PutUint64(buf[1:], segId)
	return 9
}

var DeletionRowKeySize = 1 + 8 + 8

var deletionRowValue = []byte{}

var deletionRowKeyEnd = []byte{'y'}

// ------------------------------------------------------

type DeletionRow struct {
	segId uint64
	recId uint64
}

func (p *DeletionRow) Key() []byte {
	buf := make([]byte, p.KeySize())
	size, _ := p.KeyTo(buf)
	return buf[:size]
}

func (p *DeletionRow) KeySize() int {
	return DeletionRowKeySize
}

func (p *DeletionRow) KeyTo(buf []byte) (int, error) {
	used := DeletionRowKeyPrefix(p.segId, buf)
	binary.BigEndian.PutUint64(buf[used:], p.recId)
	return used + 8, nil
}

func (p *DeletionRow) Value() []byte {
	return deletionRowValue
}

func (p *DeletionRow) ValueSize() int {
	return 0
}

func (p *DeletionRow) ValueTo(buf []byte) (int, error) {
	return 0, nil
}

func (p *DeletionRow) String() string {
	return fmt.Sprintf("Deletion segId: %d, recId: %d", p.segId, p.recId)
}

func NewDeletionRow(segId uint64, recId uint64) *DeletionRow {
	return &DeletionRow{
		segId: segId,
		recId: recId,
	}
}

func NewDeletionRowK(key []byte) (*DeletionRow, error) {
	rv := &DeletionRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (p *DeletionRow) parseK(key []byte) error {
	p.segId = binary.BigEndian.Uint64(key[1 : 1+8])
	p.recId = binary.BigEndian.Uint64(key[1+8 : 1+8+8])
	return nil
}

func NewDeletionRowKV(key, value []byte) (*DeletionRow, error) {
	return NewDeletionRowK(key)
}
