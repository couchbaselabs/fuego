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
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
)

type BackIndexRow struct {
	doc           []byte
	termEntries   []*BackIndexTermEntry
	storedEntries []*BackIndexStoreEntry
}

func (br *BackIndexRow) AllTermKeys() [][]byte {
	if br == nil {
		return nil
	}
	rv := make([][]byte, len(br.termEntries))
	for i, termEntry := range br.termEntries {
		termRow := NewTermFrequencyRow([]byte(termEntry.GetTerm()),
			uint16(termEntry.GetField()), br.doc, 0, 0)
		rv[i] = termRow.Key()
	}
	return rv
}

func (br *BackIndexRow) AllStoredKeys() [][]byte {
	if br == nil {
		return nil
	}
	rv := make([][]byte, len(br.storedEntries))
	for i, storedEntry := range br.storedEntries {
		storedRow := NewStoredRow(br.doc, uint16(storedEntry.GetField()), storedEntry.GetArrayPositions(), 'x', []byte{})
		rv[i] = storedRow.Key()
	}
	return rv
}

func (br *BackIndexRow) Key() []byte {
	buf := make([]byte, br.KeySize())
	size, _ := br.KeyTo(buf)
	return buf[:size]
}

func (br *BackIndexRow) KeySize() int {
	return len(br.doc) + 1
}

func (br *BackIndexRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'b'
	used := copy(buf[1:], br.doc)
	return used + 1, nil
}

func (br *BackIndexRow) Value() []byte {
	buf := make([]byte, br.ValueSize())
	size, _ := br.ValueTo(buf)
	return buf[:size]
}

func (br *BackIndexRow) ValueSize() int {
	birv := &BackIndexRowValue{
		TermEntries:   br.termEntries,
		StoredEntries: br.storedEntries,
	}
	return birv.Size()
}

func (br *BackIndexRow) ValueTo(buf []byte) (int, error) {
	birv := &BackIndexRowValue{
		TermEntries:   br.termEntries,
		StoredEntries: br.storedEntries,
	}
	return birv.MarshalTo(buf)
}

func (br *BackIndexRow) String() string {
	return fmt.Sprintf("Backindex DocId: `%s` Term Entries: %v, Stored Entries: %v", string(br.doc), br.termEntries, br.storedEntries)
}

func NewBackIndexRow(docID []byte, entries []*BackIndexTermEntry, storedFields []*BackIndexStoreEntry) *BackIndexRow {
	return &BackIndexRow{
		doc:           docID,
		termEntries:   entries,
		storedEntries: storedFields,
	}
}

func NewBackIndexRowKV(key, value []byte) (*BackIndexRow, error) {
	rv := BackIndexRow{}

	buf := bytes.NewBuffer(key)
	_, err := buf.ReadByte() // type
	if err != nil {
		return nil, err
	}

	rv.doc, err = buf.ReadBytes(ByteSeparator)
	if err == io.EOF && len(rv.doc) < 1 {
		err = fmt.Errorf("invalid doc length 0 - % x", key)
	}
	if err != nil && err != io.EOF {
		return nil, err
	} else if err == nil {
		rv.doc = rv.doc[:len(rv.doc)-1] // trim off separator byte
	}

	var birv BackIndexRowValue
	err = proto.Unmarshal(value, &birv)
	if err != nil {
		return nil, err
	}
	rv.termEntries = birv.TermEntries
	rv.storedEntries = birv.StoredEntries

	return &rv, nil
}
