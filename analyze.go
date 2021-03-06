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
	"github.com/blevesearch/bleve/index"

	"github.com/golang/protobuf/proto"
)

type fieldAnalysis struct {
	name               string
	length             int
	tokenFreqs         analysis.TokenFrequencies
	includeTermVectors bool
}

func (udc *Fuego) Analyze(d *document.Document) *index.AnalysisResult {
	rv := &index.AnalysisResult{
		DocID: d.ID,
		Rows:  make([]index.IndexRow, 0, 100),
	}

	docIDBytes := []byte(d.ID)

	// track our back index entries
	var backIndexStoredEntries []*BackIndexStoreEntry

	// information we collate as we merge fields with same name
	fieldAnalyses := make(map[uint16]*fieldAnalysis, len(d.Fields)+len(d.CompositeFields))

	analyzeField := func(field document.Field, storable bool) {
		name := field.Name()

		fieldIndex, newFieldRow := udc.fieldIndexOrNewRow(name)
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
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
				fa.tokenFreqs.MergeAll(fa.name, tokenFreqs)
			}

			fa.length += fieldLength

			fa.includeTermVectors = field.Options().IncludeTermVectors()
		}

		if storable && field.Options().IsStored() {
			rv.Rows, backIndexStoredEntries =
				udc.storeField(docIDBytes, field, fieldIndex, rv.Rows, backIndexStoredEntries)
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

	rowsCapNeeded := len(rv.Rows) + 1
	for _, fa := range fieldAnalyses {
		rowsCapNeeded += len(fa.tokenFreqs)
	}

	rv.Rows = append(make([]index.IndexRow, 0, rowsCapNeeded), rv.Rows...)

	backIndexTermEntries := make([]*BackIndexTermEntry, 0, rowsCapNeeded)

	// walk through the collated information and process
	// once for each indexed field (unique name)
	for fieldIndex, fa := range fieldAnalyses {
		if fa.tokenFreqs != nil {
			// encode this field
			rv.Rows, backIndexTermEntries = udc.indexField(docIDBytes,
				fa.includeTermVectors, fieldIndex, fa.length, fa.tokenFreqs,
				rv.Rows, backIndexTermEntries)
		}
	}

	// build the back index row
	backIndexRow := NewBackIndexRow(docIDBytes, 0, 0, backIndexTermEntries, backIndexStoredEntries)
	rv.Rows = append(rv.Rows, backIndexRow)

	return rv
}

func (udc *Fuego) indexField(docID []byte, includeTermVectors bool,
	fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies,
	rows []index.IndexRow, backIndexTermEntries []*BackIndexTermEntry) (
	[]index.IndexRow, []*BackIndexTermEntry) {
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for k, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow
		if includeTermVectors {
			var tv []*TermVector
			tv, rows = udc.termVectorsFromTokenFreq(fieldIndex, tf, rows)
			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docID,
				uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docID,
				uint64(frequencyFromTokenFreq(tf)), fieldNorm)
		}

		// record the back index entry
		backIndexTermEntry := BackIndexTermEntry{
			Term:  proto.String(k),
			Field: proto.Uint32(uint32(fieldIndex)),
		}
		backIndexTermEntries = append(backIndexTermEntries, &backIndexTermEntry)

		rows = append(rows, termFreqRow)
	}

	return rows, backIndexTermEntries
}

func (udc *Fuego) termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq,
	rows []index.IndexRow) (
	[]*TermVector, []index.IndexRow) {
	rv := make([]*TermVector, len(tf.Locations))

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = udc.fieldIndexOrNewRow(l.Field)
			if newFieldRow != nil {
				rows = append(rows, newFieldRow)
			}
		}
		tv := TermVector{
			field:          fieldIndex,
			arrayPositions: l.ArrayPositions,
			pos:            uint64(l.Position),
			start:          uint64(l.Start),
			end:            uint64(l.End),
		}
		rv[i] = &tv
	}

	return rv, rows
}

func frequencyFromTokenFreq(tf *analysis.TokenFreq) int {
	return tf.Frequency()
}

func (udc *Fuego) fieldIndexOrNewRow(name string) (uint16, *FieldRow) {
	index, existed := udc.fieldCache.FieldNamed(name, true)
	if !existed {
		return index, NewFieldRow(index, name)
	}
	return index, nil
}

func (udc *Fuego) storeField(docID []byte, field document.Field, fieldIndex uint16,
	rows []index.IndexRow, backIndexStoredEntries []*BackIndexStoreEntry) (
	[]index.IndexRow, []*BackIndexStoreEntry) {
	fieldType := encodeFieldType(field)
	storedRow := NewStoredRow(docID, fieldIndex, field.ArrayPositions(), fieldType, field.Value())

	// record the back index entry
	backIndexStoredEntry := BackIndexStoreEntry{
		Field:          proto.Uint32(uint32(fieldIndex)),
		ArrayPositions: field.ArrayPositions(),
	}

	return append(rows, storedRow), append(backIndexStoredEntries, &backIndexStoredEntry)
}

func encodeFieldType(f document.Field) byte {
	fieldType := byte('x')
	switch f.(type) {
	case *document.TextField:
		fieldType = 't'
	case *document.NumericField:
		fieldType = 'n'
	case *document.DateTimeField:
		fieldType = 'd'
	case *document.BooleanField:
		fieldType = 'b'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}
