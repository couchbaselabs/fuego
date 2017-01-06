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
)

type FieldRow struct {
	index uint16
	name  string
}

func (f *FieldRow) Key() []byte {
	buf := make([]byte, f.KeySize())
	size, _ := f.KeyTo(buf)
	return buf[:size]
}

func (f *FieldRow) KeySize() int {
	return 3
}

func (f *FieldRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'f'
	binary.LittleEndian.PutUint16(buf[1:3], f.index)
	return 3, nil
}

func (f *FieldRow) Value() []byte {
	return append([]byte(f.name), ByteSeparator)
}

func (f *FieldRow) ValueSize() int {
	return len(f.name) + 1
}

func (f *FieldRow) ValueTo(buf []byte) (int, error) {
	size := copy(buf, f.name)
	buf[size] = ByteSeparator
	return size + 1, nil
}

func (f *FieldRow) String() string {
	return fmt.Sprintf("Field: %d Name: %s", f.index, f.name)
}

func NewFieldRow(index uint16, name string) *FieldRow {
	return &FieldRow{
		index: index,
		name:  name,
	}
}

func NewFieldRowKV(key, value []byte) (*FieldRow, error) {
	rv := FieldRow{}

	buf := bytes.NewBuffer(key)
	_, err := buf.ReadByte() // type
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &rv.index)
	if err != nil {
		return nil, err
	}

	buf = bytes.NewBuffer(value)
	rv.name, err = buf.ReadString(ByteSeparator)
	if err != nil {
		return nil, err
	}
	rv.name = rv.name[:len(rv.name)-1] // trim off separator byte

	return &rv, nil
}
