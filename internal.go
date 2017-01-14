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

func (udc *Fuego) SetInternal(key, val []byte) error {
	internalRow := NewInternalRow(key, val)

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	writer, err := udc.store.Writer()
	if err != nil {
		return
	}
	defer writer.Close()

	batch := writer.NewBatch()
	batch.Set(internalRow.Key(), internalRow.Value())

	return writer.ExecuteBatch(batch)
}

func (udc *Fuego) DeleteInternal(key []byte) error {
	internalRow := NewInternalRow(key, nil)

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	writer, err := udc.store.Writer()
	if err != nil {
		return
	}
	defer writer.Close()

	batch := writer.NewBatch()
	batch.Delete(internalRow.Key())

	return writer.ExecuteBatch(batch)
}
