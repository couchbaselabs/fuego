//  Copyright (c) 2015 Couchbase, Inc.
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
	"math"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"

	"github.com/golang/protobuf/proto"
)

type analyzeAuxResult struct {
	docID      string
	docIDBytes []byte

	fieldRows    []*FieldRow
	termFreqRows []*TermFrequencyRow
	storedRows   []*StoredRow
	backIndexRow *BackIndexRow
}

func (udc *Fuego) analyzeAux(d *document.Document, prealloc *analyzeAuxResult) *analyzeAuxResult {
	rv := prealloc
	if rv == nil {
		rv = &analyzeAuxResult{}
	}
	rv.docID = d.ID
	rv.docIDBytes = []byte(d.ID)

	// track our back index entries
	backIndexStoreEntries := []*BackIndexStoreEntry{}

	// information we collate as we merge fields with same name
	fieldTermFreqs := make(map[uint16]analysis.TokenFrequencies)
	fieldLengths := make(map[uint16]int)
	fieldIncludeTermVectors := make(map[uint16]bool)
	fieldNames := make(map[uint16]string)

	analyzeField := func(field document.Field, storable bool) {
		fieldIndex, newFieldRow := udc.fieldIndexOrNewRow(field.Name())
		if newFieldRow != nil {
			rv.fieldRows = append(rv.fieldRows, newFieldRow)
		}

		fieldNames[fieldIndex] = field.Name()

		if field.Options().IsIndexed() {
			fieldLength, tokenFreqs := field.Analyze()

			existingFreqs := fieldTermFreqs[fieldIndex]
			if existingFreqs == nil {
				fieldTermFreqs[fieldIndex] = tokenFreqs
			} else {
				existingFreqs.MergeAll(field.Name(), tokenFreqs)
				fieldTermFreqs[fieldIndex] = existingFreqs
			}

			fieldLengths[fieldIndex] += fieldLength

			fieldIncludeTermVectors[fieldIndex] = field.Options().IncludeTermVectors()
		}

		if storable && field.Options().IsStored() {
			rv.storedRows, backIndexStoreEntries = udc.storeFieldAux(rv.docIDBytes,
				field, fieldIndex, rv.storedRows, backIndexStoreEntries)
		}
	}

	// walk all the fields, record stored fields now
	// place information about indexed fields into map
	// this collates information across fields with
	// same names (arrays)
	for _, field := range d.Fields {
		analyzeField(field, true)
	}

	if len(d.CompositeFields) > 0 {
		for fieldIndex, tokenFreqs := range fieldTermFreqs {
			// see if any of the composite fields need this
			for _, compositeField := range d.CompositeFields {
				compositeField.Compose(fieldNames[fieldIndex], fieldLengths[fieldIndex], tokenFreqs)
			}
		}

		for _, compositeField := range d.CompositeFields {
			analyzeField(compositeField, false)
		}
	}

	numTokenFreqs := 0
	for _, tokenFreqs := range fieldTermFreqs {
		numTokenFreqs += len(tokenFreqs)
	}

	if rv.termFreqRows == nil ||
		cap(rv.termFreqRows) < numTokenFreqs {
		rv.termFreqRows = make([]*TermFrequencyRow, 0, numTokenFreqs)
	}
	rv.termFreqRows = rv.termFreqRows[0:0]

	backIndexTermEntries := make([]*BackIndexTermEntry, 0, numTokenFreqs)

	// walk through the collated information and process
	// once for each indexed field (unique name)
	for fieldIndex, tokenFreqs := range fieldTermFreqs {
		fieldLength := fieldLengths[fieldIndex]
		includeTermVectors := fieldIncludeTermVectors[fieldIndex]

		// encode this field
		rv.fieldRows, rv.termFreqRows, backIndexTermEntries = udc.indexFieldAux(rv.docIDBytes,
			includeTermVectors, fieldIndex, fieldLength, tokenFreqs,
			rv.fieldRows, rv.termFreqRows, backIndexTermEntries)
	}

	// build the back index row
	rv.backIndexRow = NewBackIndexRow(rv.docIDBytes, backIndexTermEntries, backIndexStoreEntries)

	return rv
}

func (udc *Fuego) indexFieldAux(docID []byte, includeTermVectors bool,
	fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies,
	fieldRows []*FieldRow,
	termFreqRows []*TermFrequencyRow,
	backIndexTermEntries []*BackIndexTermEntry) (
	[]*FieldRow, []*TermFrequencyRow, []*BackIndexTermEntry) {
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for k, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow

		if includeTermVectors {
			var tv []*TermVector
			tv, fieldRows = udc.termVectorsFromTokenFreqAux(fieldIndex, tf, fieldRows)

			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docID,
				uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docID,
				uint64(frequencyFromTokenFreq(tf)), fieldNorm)
		}

		// record the back index entry
		backIndexTermEntries = append(backIndexTermEntries, &BackIndexTermEntry{
			Term:  proto.String(k),
			Field: proto.Uint32(uint32(fieldIndex)),
		})

		termFreqRows = append(termFreqRows, termFreqRow)
	}

	return fieldRows, termFreqRows, backIndexTermEntries
}

func (udc *Fuego) termVectorsFromTokenFreqAux(field uint16,
	tf *analysis.TokenFreq, fieldRows []*FieldRow) ([]*TermVector, []*FieldRow) {
	rv := make([]*TermVector, len(tf.Locations))

	for i, l := range tf.Locations {
		fieldIndex := field
		if l.Field != "" {
			var newFieldRow *FieldRow

			fieldIndex, newFieldRow = udc.fieldIndexOrNewRow(l.Field)
			if newFieldRow != nil {
				fieldRows = append(fieldRows, newFieldRow)
			}
		}

		rv[i] = &TermVector{
			field:          fieldIndex,
			arrayPositions: l.ArrayPositions,
			pos:            uint64(l.Position),
			start:          uint64(l.Start),
			end:            uint64(l.End),
		}
	}

	return rv, fieldRows
}

func (udc *Fuego) storeFieldAux(docID []byte, field document.Field, fieldIndex uint16,
	storedRows []*StoredRow, backIndexStoreEntries []*BackIndexStoreEntry) (
	[]*StoredRow, []*BackIndexStoreEntry) {
	fieldType := encodeFieldType(field)

	storedRows = append(storedRows, NewStoredRow(docID,
		fieldIndex, field.ArrayPositions(), fieldType, field.Value()))

	backIndexStoreEntries = append(backIndexStoreEntries, &BackIndexStoreEntry{
		Field:          proto.Uint32(uint32(fieldIndex)),
		ArrayPositions: field.ArrayPositions(),
	})

	return storedRows, backIndexStoreEntries
}

