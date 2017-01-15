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

var SummaryKey = []byte{'z'}

type SummaryRow struct {
	// Decreases from MAX_UINT64 downwards, so that newer, younger
	// seg's appear earlier in key-val iterators.
	LastUsedSegId uint64
}

func (dr *SummaryRow) Key() []byte {
	return SummaryKey
}

func (dr *SummaryRow) KeySize() int {
	return 1
}

func (dr *SummaryRow) KeyTo(buf []byte) (int, error) {
	buf[0] = SummaryKey[0]
	return 1, nil
}

func (dr *SummaryRow) Value() []byte {
	buf := make([]byte, dr.ValueSize())
	size, _ := dr.ValueTo(buf)
	return buf[:size]
}

func (dr *SummaryRow) ValueSize() int {
	return 8
}

func (dr *SummaryRow) ValueTo(buf []byte) (int, error) {
	binary.LittleEndian.PutUint64(buf, dr.LastUsedSegId)
	return 8, nil
}

func (dr *SummaryRow) String() string {
	return fmt.Sprintf("Summary: LastUsedSegId: %d", dr.LastUsedSegId)
}

func NewSummaryRow(lastUsedSegId uint64) *SummaryRow {
	return &SummaryRow{LastUsedSegId: lastUsedSegId}
}

func NewSummaryRowKV(key, value []byte) (*SummaryRow, error) {
	return &SummaryRow{
		LastUsedSegId: binary.LittleEndian.Uint64(value),
	}, nil
}
