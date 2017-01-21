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
	"encoding/binary"
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
	segPostingsArr     []*segPostings
	tmpIdRow           IdRow
	field              uint16
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
}

type segPostings struct {
	rowRecIds    *PostingRecIdsRow
	rowFreqNorms *PostingFreqNormsRow
	rowVecs      *PostingVecsRow

	idIter store.KVIterator // Iterates through id lookup rows.

	nextRecIdx int // The next recId by 0-based position.
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

	segPostingsArr, err := loadSegPostingsArr(indexReader.kvreader, field, term)
	if err != nil {
		return nil, err
	}

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
		segPostingsArr:     segPostingsArr,
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
	*index.TermFieldDoc, error) {
	if r.iter != nil {
		if r.tfrNext == nil {
			r.tfrNext = &TermFrequencyRow{}
		}

		tfr := InitTermFrequencyRow(r.tfrNext, r.term, r.field, docID, 0, 0)
		keyBuf, err := tfr.KeyAppendTo(r.keyBuf[:0])
		if err != nil {
			return nil, err
		}

		r.iter.Seek(keyBuf)

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

func (r *TermFieldReader) Close() error {
	if r.indexReader != nil {
		atomic.AddUint64(&r.indexReader.index.stats.termSearchersFinished, uint64(1))
	}

	if r.iter != nil {
		r.iter.Close()
		r.iter = nil
	}

	closeSegPostingsArr(r.segPostingsArr)

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

func closeSegPostingsArr(arr []*segPostings) {
	for _, sp := range arr {
		if sp.idIter != nil {
			sp.idIter.Close()
			sp.idIter = nil
		}
	}
}

func loadSegPostingsArr(kvreader store.KVReader, field uint16, term []byte) ([]*segPostings, error) {
	bufSize := PostingRowKeySize(term)
	if bufSize < IdRowKeySize {
		bufSize = IdRowKeySize
	}

	buf := make([]byte, bufSize)
	bufUsed := PostingRowKeyPrefix(field, term, buf)
	bufPrefix := buf[:bufUsed]

	it := kvreader.PrefixIterator(bufPrefix)
	defer it.Close()

	var rv []*segPostings

	k, v, valid := it.Current()
	for valid {
		rowRecIds, err := NewPostingRecIdsRowKV(k, v)
		if err != nil {
			closeSegPostingsArr(rv)
			return nil, err
		}

		it.Next()
		k, v, valid = it.Current()
		if !valid {
			closeSegPostingsArr(rv)
			return nil, fmt.Errorf("expected postingFreqNormsRow")
		}

		rowFreqNorms, err := NewPostingFreqNormsRowKV(k, v)
		if err != nil {
			closeSegPostingsArr(rv)
			return nil, err
		}
		if rowFreqNorms.segId != rowRecIds.segId {
			closeSegPostingsArr(rv)
			return nil, fmt.Errorf("mismatched segId's for postingFreqNormsRow")
		}

		it.Next()
		k, v, valid = it.Current()
		if !valid {
			closeSegPostingsArr(rv)
			return nil, fmt.Errorf("expected postingVecsRow")
		}

		rowVecs, err := NewPostingVecsRowKV(k, v)
		if err != nil {
			closeSegPostingsArr(rv)
			return nil, err
		}
		if rowVecs.segId != rowRecIds.segId {
			closeSegPostingsArr(rv)
			return nil, fmt.Errorf("mismatched segId's for postingVecsRow")
		}

		it.Next()
		k, v, valid = it.Current()

		bufIdUsed := IdRowKeyPrefix(rowRecIds.segId, buf)
		bufIdPrefix := buf[:bufIdUsed]

		idIter := kvreader.PrefixIterator(bufIdPrefix)

		rv = append(rv, &segPostings{
			rowRecIds:    rowRecIds,
			rowFreqNorms: rowFreqNorms,
			rowVecs:      rowVecs,
			idIter:       idIter,
		})
	}

	return rv, nil
}

func (r *TermFieldReader) PostingsNext(preAlloced *index.TermFieldDoc) (
	*index.TermFieldDoc, error) {
LOOP_SEG:
	for len(r.segPostingsArr) > 0 {
		sp := r.segPostingsArr[0]

	LOOP_IDITER:
		for true {
			idRowKey, _, valid := sp.idIter.Current()
			if !valid {
				r.segPostingsArr = r.segPostingsArr[1:]
				sp.idIter.Close()
				sp.idIter = nil
				continue LOOP_SEG
			}

			idRow := &r.tmpIdRow
			err := idRow.parseK(idRowKey)
			if err != nil {
				return nil, err
			}

		LOOP_REC:
			for true {
				if sp.nextRecIdx >= len(sp.rowRecIds.recIds) {
					r.segPostingsArr = r.segPostingsArr[1:]
					sp.idIter.Close()
					sp.idIter = nil
					continue LOOP_SEG
				}

				recIdx := sp.nextRecIdx
				sp.nextRecIdx++

				recId := sp.rowRecIds.recIds[recIdx]

				if idRow.recId < recId {
					idRow.segId = sp.rowRecIds.segId
					idRow.recId = recId
					if cap(r.keyBuf) < IdRowKeySize {
						r.keyBuf = make([]byte, IdRowKeySize)
					}
					keyBuf := r.keyBuf[0:IdRowKeySize]
					keyBufUsed, _ := idRow.KeyTo(keyBuf)
					sp.idIter.Seek(keyBuf[:keyBufUsed])
					continue LOOP_IDITER
				}

				if idRow.recId > recId {
					continue LOOP_REC // The idRow was deleted.
				}

				// The idRow.recId == recId, so found a result.
				//
				rv := preAlloced
				if rv == nil {
					rv = &index.TermFieldDoc{}
				}

				// Strip off idRow 1-byte prefix.
				rv.ID = append(rv.ID, idRowKey[1:]...)

				if r.includeFreq {
					rv.Freq = uint64(sp.rowFreqNorms.Freq(recIdx))
				}

				if r.includeNorm {
					rv.Norm = float64(sp.rowFreqNorms.Norm(recIdx))
				}

				if r.includeTermVectors {
					tvs, err := sp.rowVecs.TermVectors(recIdx, nil)
					if err != nil {
						return nil, err
					}

					rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tvs)
				}
			}
		}
	}

	return nil, nil
}

func (r *TermFieldReader) PostingsAdvance(wantId index.IndexInternalID,
	preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	wantSegId := binary.LittleEndian.Uint64(wantId[:8])
	wantRecId := binary.LittleEndian.Uint64(wantId[8:8+8])

LOOP_SEG:
	for len(r.segPostingsArr) > 0 {
		sp := r.segPostingsArr[0]
		if sp.rowRecIds.segId < wantSegId {
			r.segPostingsArr = r.segPostingsArr[1:]
			sp.idIter.Close()
			sp.idIter = nil
			continue LOOP_SEG
		}

	LOOP_IDITER:
		for true {
			idRowKey, _, valid := sp.idIter.Current()
			if !valid {
				r.segPostingsArr = r.segPostingsArr[1:]
				sp.idIter.Close()
				sp.idIter = nil
				continue LOOP_SEG
			}

			idRow := &r.tmpIdRow
			err := idRow.parseK(idRowKey)
			if err != nil {
				return nil, err
			}

			if idRow.segId == wantSegId &&
				idRow.recId < wantRecId {
				idRow.recId = wantRecId
				if cap(r.keyBuf) < IdRowKeySize {
					r.keyBuf = make([]byte, IdRowKeySize)
				}
				keyBuf := r.keyBuf[0:IdRowKeySize]
				keyBufUsed, _ := idRow.KeyTo(keyBuf)
				sp.idIter.Seek(keyBuf[:keyBufUsed])
				continue LOOP_IDITER
			}

		LOOP_REC:
			for true {
				if sp.nextRecIdx >= len(sp.rowRecIds.recIds) {
					r.segPostingsArr = r.segPostingsArr[1:]
					sp.idIter.Close()
					sp.idIter = nil
					continue LOOP_SEG
				}

				recIdx := sp.nextRecIdx
				sp.nextRecIdx++

				recId := sp.rowRecIds.recIds[recIdx]

				if idRow.recId < recId {
					idRow.segId = sp.rowRecIds.segId
					idRow.recId = recId
					if cap(r.keyBuf) < IdRowKeySize {
						r.keyBuf = make([]byte, IdRowKeySize)
					}
					keyBuf := r.keyBuf[0:IdRowKeySize]
					keyBufUsed, _ := idRow.KeyTo(keyBuf)
					sp.idIter.Seek(keyBuf[:keyBufUsed])
					continue LOOP_IDITER
				}

				if idRow.recId > recId {
					continue LOOP_REC // The idRow was deleted.
				}

				// The idRow.recId == recId, so found a result.
				//
				rv := preAlloced
				if rv == nil {
					rv = &index.TermFieldDoc{}
				}

				// Strip off idRow 1-byte prefix.
				rv.ID = append(rv.ID, idRowKey[1:]...)

				if r.includeFreq {
					rv.Freq = uint64(sp.rowFreqNorms.Freq(recIdx))
				}

				if r.includeNorm {
					rv.Norm = float64(sp.rowFreqNorms.Norm(recIdx))
				}

				if r.includeTermVectors {
					tvs, err := sp.rowVecs.TermVectors(recIdx, nil)
					if err != nil {
						return nil, err
					}

					rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tvs)
				}
			}
		}
	}

	return nil, nil
}
