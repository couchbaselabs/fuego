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

type VersionRow struct {
	version uint8
}

func (v *VersionRow) Key() []byte {
	return []byte{'v'}
}

func (v *VersionRow) KeySize() int {
	return 1
}

func (v *VersionRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'v'
	return 1, nil
}

func (v *VersionRow) Value() []byte {
	return []byte{byte(v.version)}
}

func (v *VersionRow) ValueSize() int {
	return 1
}

func (v *VersionRow) ValueTo(buf []byte) (int, error) {
	buf[0] = v.version
	return 1, nil
}

func (v *VersionRow) String() string {
	return fmt.Sprintf("Version: %d", v.version)
}

func NewVersionRow(version uint8) *VersionRow {
	return &VersionRow{version: version}
}

func NewVersionRowKV(key, value []byte) (*VersionRow, error) {
	rv := VersionRow{}
	buf := bytes.NewBuffer(value)
	err := binary.Read(buf, binary.LittleEndian, &rv.version)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}
