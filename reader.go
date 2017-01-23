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
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type IndexReader struct {
	index    *Fuego
	kvreader store.KVReader
	docCount uint64
}

func (i *IndexReader) TermFieldReader(term []byte, fieldName string,
	includeFreq, includeNorm, includeTermVectors bool) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := i.index.fieldCache.FieldNamed(fieldName, false)
	if fieldExists {
		return newTermFieldReader(i, term, uint16(fieldIndex),
			includeFreq, includeNorm, includeTermVectors)
	}

	return newTermFieldReader(i, []byte{ByteSeparator}, ^uint16(0),
		includeFreq, includeNorm, includeTermVectors)
}

func (i *IndexReader) FieldDict(fieldName string) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, nil, nil)
}

func (i *IndexReader) FieldDictRange(fieldName string, startTerm []byte, endTerm []byte) (
	index.FieldDict, error) {
	fieldIndex, fieldExists := i.index.fieldCache.FieldNamed(fieldName, false)
	if fieldExists {
		return newFieldDict(i, uint16(fieldIndex), startTerm, endTerm)
	}

	return newFieldDict(i, ^uint16(0), []byte{ByteSeparator}, []byte{})
}

func (i *IndexReader) FieldDictPrefix(fieldName string, termPrefix []byte) (
	index.FieldDict, error) {
	return i.FieldDictRange(fieldName, termPrefix, termPrefix)
}

func (i *IndexReader) DocIDReaderAll() (index.DocIDReader, error) {
	return newIdReader(i)
}

func (i *IndexReader) DocIDReaderOnly(docIDs []string) (index.DocIDReader, error) {
	return newIdSetReaderFromDocIDs(i, docIDs)
}

func (i *IndexReader) Document(docID string) (*document.Document, error) {
	docIDBytes := []byte(docID)

	// first hit the back index to confirm doc exists
	backIndexRow, err := backIndexRowForDocID(i.kvreader, docIDBytes, nil)
	if err != nil || backIndexRow == nil {
		return nil, err
	}

	doc := document.NewDocument(docID)

	storedRow := NewStoredRow(docIDBytes, 0, nil, 'x', nil)
	storedRowScanPrefix := storedRow.ScanPrefixForDoc()

	it := i.kvreader.PrefixIterator(storedRowScanPrefix)
	defer it.Close()

	key, val, valid := it.Current()
	for valid {
		safeVal := make([]byte, len(val))
		copy(safeVal, val)

		row, err := NewStoredRowKV(key, safeVal)
		if err != nil {
			return nil, err
		}

		if row != nil {
			fieldName := i.index.fieldCache.FieldIndexed(row.field)

			field := decodeFieldType(row.typ, fieldName, row.arrayPositions, row.value)
			if field != nil {
				doc.AddField(field)
			}
		}

		it.Next()
		key, val, valid = it.Current()
	}

	return doc, nil
}

func decodeFieldType(typ byte, name string, pos []uint64, value []byte) document.Field {
	switch typ {
	case 't':
		return document.NewTextField(name, pos, value)
	case 'n':
		return document.NewNumericFieldFromBytes(name, pos, value)
	case 'd':
		return document.NewDateTimeFieldFromBytes(name, pos, value)
	case 'b':
		return document.NewBooleanFieldFromBytes(name, pos, value)
	}

	return nil
}

func (i *IndexReader) DocumentFieldTerms(internalId index.IndexInternalID, fields []string) (
	index.FieldTerms, error) {
	docIDBytes, err := i.ExternalIDBytes(internalId)
	if err != nil {
		return nil, err
	}

	back, err := backIndexRowForDocID(i.kvreader, docIDBytes, nil)
	if err != nil || back == nil {
		return nil, err
	}

	rv := make(index.FieldTerms, len(fields))
	fieldsMap := make(map[uint16]string, len(fields))

	for _, f := range fields {
		id, ok := i.index.fieldCache.FieldNamed(f, false)
		if ok {
			fieldsMap[id] = f
		}
	}

	for _, entry := range back.termEntries {
		if field, ok := fieldsMap[uint16(*entry.Field)]; ok {
			rv[field] = append(rv[field], *entry.Term)
		}
	}

	return rv, nil
}

func (i *IndexReader) Fields() (fields []string, err error) {
	fields = make([]string, 0)

	it := i.kvreader.PrefixIterator([]byte{'f'})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	key, val, valid := it.Current()
	for valid {
		var row KVRow
		row, err = ParseFromKeyValue(key, val)
		if err != nil {
			fields = nil
			return
		}
		if row != nil {
			fieldRow, ok := row.(*FieldRow)
			if ok {
				fields = append(fields, fieldRow.name)
			}
		}

		it.Next()
		key, val, valid = it.Current()
	}

	return
}

func (i *IndexReader) GetInternal(key []byte) ([]byte, error) {
	internalRow := NewInternalRow(key, nil)
	return i.kvreader.Get(internalRow.Key())
}

func (i *IndexReader) DocCount() (uint64, error) {
	return i.docCount, nil
}

func (i *IndexReader) Close() error {
	return i.kvreader.Close()
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)

	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			break // didn't overflow, so stop
		}
	}

	return rv
}
