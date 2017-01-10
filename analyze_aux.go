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
	var backIndexStoreEntries []*BackIndexStoreEntry

	// information we collate as we merge fields with same name
	fieldAnalyses := make(map[uint16]*fieldAnalysis, len(d.Fields)+len(d.CompositeFields))

	analyzeField := func(field document.Field, storable bool) {
		name := field.Name()

		fieldIndex, newFieldRow := udc.fieldIndexOrNewRow(name)
		if newFieldRow != nil {
			rv.fieldRows = append(rv.fieldRows, newFieldRow)
		}

		fa := fieldAnalyses[fieldIndex]
		if fa == nil {
			fa = &fieldAnalysis{name: name}
			fieldAnalyses[fieldIndex] = fa
		}

		if field.Options().IsIndexed() {
			fieldLength, tokenFreqs := field.Analyze()

			if fa.tokenFreqs == nil {
				fa.tokenFreqs = tokenFreqs
			} else {
				fa.tokenFreqs.MergeAll(name, tokenFreqs)
			}

			fa.length += fieldLength

			fa.includeTermVectors = field.Options().IncludeTermVectors()
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
		for _, fa := range fieldAnalyses {
			if fa.tokenFreqs != nil {
				// see if any of the composite fields need this
				for _, compositeField := range d.CompositeFields {
					compositeField.Compose(fa.name, fa.length, fa.tokenFreqs)
				}
			}
		}

		for _, compositeField := range d.CompositeFields {
			analyzeField(compositeField, false)
		}
	}

	numTokenFreqs := 0
	for _, fa := range fieldAnalyses {
		numTokenFreqs += len(fa.tokenFreqs)
	}

	if rv.termFreqRows == nil || cap(rv.termFreqRows) < numTokenFreqs {
		rv.termFreqRows = make([]*TermFrequencyRow, 0, numTokenFreqs)
	}
	rv.termFreqRows = rv.termFreqRows[0:0]

	backIndexTermEntries := make([]*BackIndexTermEntry, 0, numTokenFreqs)

	// walk through the collated information and process
	// once for each indexed field (unique name)
	for fieldIndex, fa := range fieldAnalyses {
		if fa.tokenFreqs != nil {
			// encode this field
			rv.fieldRows, rv.termFreqRows, backIndexTermEntries = udc.indexFieldAux(rv.docIDBytes,
				fa.includeTermVectors, fieldIndex, fa.length, fa.tokenFreqs,
				rv.fieldRows, rv.termFreqRows, backIndexTermEntries)
		}
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

	termFreqRowsArr := make([]TermFrequencyRow, len(tokenFreqs))
	termFreqRowsUsed := 0

	for k, tf := range tokenFreqs {
		termFreqRow := &termFreqRowsArr[termFreqRowsUsed]
		termFreqRowsUsed++

		InitTermFrequencyRow(termFreqRow, tf.Term, fieldIndex, docID,
			uint64(frequencyFromTokenFreq(tf)), fieldNorm)

		if includeTermVectors {
			termFreqRow.vectors, fieldRows =
				udc.termVectorsFromTokenFreqAux(fieldIndex, tf, fieldRows)
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
	a := make([]TermVector, len(tf.Locations))
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

		a[i] = TermVector{
			field:          fieldIndex,
			arrayPositions: l.ArrayPositions,
			pos:            uint64(l.Position),
			start:          uint64(l.Start),
			end:            uint64(l.End),
		}
		rv[i] = &a[i]
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
