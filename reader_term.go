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
	"fmt"
	"sync/atomic"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type TermFieldReader struct {
	count              uint64
	indexReader        *IndexReader
	iter               store.KVIterator
	term               []byte
	tfrNext            *TermFrequencyRow
	keyBuf             []byte
	field              uint16
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
}

type postings struct {
	term        []byte
	segPostings []*segPostings
	field       uint16
}

type segPostings struct {
	rowRecIds    *PostingRecIdsRow
	rowFreqNorms *PostingFreqNormsRow
	rowVecs      *PostingVecsRow

	idIter store.KVIterator // Iterator through id lookup rows.

	cur int // The current recId by 0-based position.
}

// ---------------------------------------------

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
			count: 0,
			term:  term,
			field: field,
		}, nil
	}

	err = dictionaryRow.parseDictionaryV(val)
	if err != nil {
		return nil, err
	}

	postings, err := loadPostings(indexReader.kvreader, field, term)
	if err != nil {
		return nil, err
	}

	postings.Close()

	tfr := NewTermFrequencyRow(term, field, nil, 0, 0)

	iter := indexReader.kvreader.PrefixIterator(tfr.Key())

	atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))

	return &TermFieldReader{
		indexReader:        indexReader,
		iter:               iter,
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
	if r.iter != nil {
		// We treat tfrNext also like an initialization flag, which
		// tells us whether we need to invoke the underlying
		// iter.Next().  The first time, don't call iter.Next().
		if r.tfrNext != nil {
			r.iter.Next()
		} else {
			r.tfrNext = &TermFrequencyRow{}
		}

		key, val, valid := r.iter.Current()
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
	if r.iter != nil {
		if r.tfrNext == nil {
			r.tfrNext = &TermFrequencyRow{}
		}

		tfr := InitTermFrequencyRow(r.tfrNext, r.term, r.field, docID, 0, 0)
		r.keyBuf, err = tfr.KeyAppendTo(r.keyBuf[:0])
		if err != nil {
			return nil, err
		}

		r.iter.Seek(r.keyBuf)

		key, val, valid := r.iter.Current()
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

	if r.iter != nil {
		return r.iter.Close()
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

// ---------------------------------------------

func (p *postings) Close() {
	for _, sp := range p.segPostings {
		if sp.idIter != nil {
			sp.idIter.Close()
			sp.idIter = nil
		}
	}
}

func loadPostings(kvreader store.KVReader, field uint16, term []byte) (*postings, error) {
	bufSize := PostingRowKeySize(term)
	if bufSize < IdRowKeySize {
		bufSize = IdRowKeySize
	}

	buf := make([]byte, bufSize)
	bufUsed := PostingRowKeyPrefix(field, term, buf)
	bufPrefix := buf[:bufUsed]

	it := kvreader.PrefixIterator(bufPrefix)
	defer it.Close()

	rv := &postings{
		field: field,
		term:  term,
	}

	k, v, valid := it.Current()
	for valid {
		rowRecIds, err := NewPostingRecIdsRowKV(k, v)
		if err != nil {
			rv.Close()
			return nil, err
		}

		it.Next()
		k, v, valid = it.Current()
		if !valid {
			rv.Close()
			return nil, fmt.Errorf("expected postingFreqNormsRow")
		}

		rowFreqNorms, err := NewPostingFreqNormsRowKV(k, v)
		if err != nil {
			rv.Close()
			return nil, err
		}
		if rowFreqNorms.segId != rowRecIds.segId {
			rv.Close()
			return nil, fmt.Errorf("mismatched segId's for postingFreqNormsRow")
		}

		it.Next()
		k, v, valid = it.Current()
		if !valid {
			rv.Close()
			return nil, fmt.Errorf("expected postingVecsRow")
		}

		rowVecs, err := NewPostingVecsRowKV(k, v)
		if err != nil {
			rv.Close()
			return nil, err
		}
		if rowVecs.segId != rowRecIds.segId {
			rv.Close()
			return nil, fmt.Errorf("mismatched segId's for postingVecsRow")
		}

		it.Next()
		k, v, valid = it.Current()

		bufIdUsed := IdRowKeyPrefix(rowRecIds.segId, buf)
		bufIdPrefix := buf[:bufIdUsed]

		idIter := kvreader.PrefixIterator(bufIdPrefix)

		rv.segPostings = append(rv.segPostings, &segPostings{
			rowRecIds:    rowRecIds,
			rowFreqNorms: rowFreqNorms,
			rowVecs:      rowVecs,
			idIter:       idIter,
		})
	}

	return rv, nil
}
