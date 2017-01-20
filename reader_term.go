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
	"sync/atomic"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type TermFieldReader struct {
	count              uint64
	indexReader        *IndexReader
	iterator           store.KVIterator
	term               []byte
	tfrNext            *TermFrequencyRow
	keyBuf             []byte
	field              uint16
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
}

func newTermFieldReader(indexReader *IndexReader, term []byte, field uint16,
	includeFreq, includeNorm, includeTermVectors bool) (*TermFieldReader, error) {
	dictionaryRow := NewDictionaryRow(term, field, 0)

	val, err := indexReader.kvreader.Get(dictionaryRow.Key())
	if err != nil {
		return nil, err
	}

	if val == nil {
		atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
		return &TermFieldReader{
			count:              0,
			term:               term,
			tfrNext:            &TermFrequencyRow{},
			field:              field,
			includeFreq:        includeFreq,
			includeNorm:        includeNorm,
			includeTermVectors: includeTermVectors,
		}, nil
	}

	err = dictionaryRow.parseDictionaryV(val)
	if err != nil {
		return nil, err
	}

	tfr := NewTermFrequencyRow(term, field, []byte{}, 0, 0)

	it := indexReader.kvreader.PrefixIterator(tfr.Key())

	atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))

	return &TermFieldReader{
		indexReader:        indexReader,
		iterator:           it,
		count:              dictionaryRow.count,
		term:               term,
		field:              field,
		includeFreq:        includeFreq,
		includeNorm:        includeNorm,
		includeTermVectors: includeTermVectors,
	}, nil
}

func (r *TermFieldReader) Count() uint64 {
	return r.count
}

func (r *TermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if r.iterator != nil {
		// We treat tfrNext also like an initialization flag, which
		// tells us whether we need to invoke the underlying
		// iterator.Next().  The first time, don't call iterator.Next().
		if r.tfrNext != nil {
			r.iterator.Next()
		} else {
			r.tfrNext = &TermFrequencyRow{}
		}

		key, val, valid := r.iterator.Current()
		if valid {
			tfr := r.tfrNext

			err := tfr.parseKDoc(key, r.term)
			if err != nil {
				return nil, err
			}

			err = tfr.parseV(val, r.includeTermVectors)
			if err != nil {
				return nil, err
			}

			rv := preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}

			rv.ID = append(rv.ID, tfr.doc...)
			rv.Freq = tfr.freq
			rv.Norm = float64(tfr.norm)

			if tfr.vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tfr.vectors)
			}

			return rv, nil
		}
	}

	return nil, nil
}

func (r *TermFieldReader) Advance(docID index.IndexInternalID, preAlloced *index.TermFieldDoc) (
	rv *index.TermFieldDoc, err error) {
	if r.iterator != nil {
		if r.tfrNext == nil {
			r.tfrNext = &TermFrequencyRow{}
		}

		tfr := InitTermFrequencyRow(r.tfrNext, r.term, r.field, docID, 0, 0)
		r.keyBuf, err = tfr.KeyAppendTo(r.keyBuf[:0])
		if err != nil {
			return nil, err
		}

		r.iterator.Seek(r.keyBuf)

		key, val, valid := r.iterator.Current()
		if valid {
			err := tfr.parseKDoc(key, r.term)
			if err != nil {
				return nil, err
			}

			err = tfr.parseV(val, r.includeTermVectors)
			if err != nil {
				return nil, err
			}

			rv = preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}

			rv.ID = append(rv.ID, tfr.doc...)
			rv.Freq = tfr.freq
			rv.Norm = float64(tfr.norm)

			if tfr.vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tfr.vectors)
			}

			return rv, nil
		}
	}

	return nil, nil
}

func (r *TermFieldReader) Close() error {
	if r.indexReader != nil {
		atomic.AddUint64(&r.indexReader.index.stats.termSearchersFinished, uint64(1))
	}

	if r.iterator != nil {
		return r.iterator.Close()
	}

	return nil
}

func (udc *Fuego) termFieldVectorsFromTermVectors(in []*TermVector) []*index.TermFieldVector {
	if len(in) <= 0 {
		return nil
	}

	fieldName := udc.fieldCache.FieldIndexed(in[0].field)

	rv := make([]*index.TermFieldVector, len(in))
	for i, tv := range in {
		rv[i] = &index.TermFieldVector{
			Field:          fieldName,
			ArrayPositions: tv.arrayPositions,
			Pos:            tv.pos,
			Start:          tv.start,
			End:            tv.end,
		}
	}

	return rv
}
