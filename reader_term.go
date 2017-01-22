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
	indexReader    *IndexReader
	count          uint64
	term           []byte
	segPostingsArr []*segPostings
	deletionIter   store.KVIterator // Iterates through deletion rows.
	keyBuf         []byte
	tmpDeletionRow DeletionRow

	curSegPostings *segPostings
	curDeletionRow *DeletionRow

	field              uint16
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
}

type segPostings struct {
	rowRecIds    *PostingRecIdsRow
	rowFreqNorms *PostingFreqNormsRow
	rowVecs      *PostingVecsRow

	nextRecIdx int // The next recId by 0-based position.
}

// ---------------------------------------------

func newTermFieldReader(indexReader *IndexReader, term []byte, field uint16,
	includeFreq, includeNorm, includeTermVectors bool) (*TermFieldReader, error) {
	dictionaryRow := NewDictionaryRow(term, field, 0)

	keySize := dictionaryRow.KeySize()
	if keySize < DeletionRowKeySize {
		keySize = DeletionRowKeySize
	}
	keyBuf := make([]byte, keySize)

	keyUsed, _ := dictionaryRow.KeyTo(keyBuf)
	val, err := indexReader.kvreader.Get(keyBuf[:keyUsed])
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

	var curSegPostings *segPostings
	var deletionIter store.KVIterator

	if len(segPostingsArr) > 0 {
		curSegPostings = segPostingsArr[0]

		segId := curSegPostings.rowRecIds.segId

		keyUsed := DeletionRowKeyPrefix(segId, keyBuf)
		keyStart := keyBuf[:keyUsed]

		deletionIter =
			indexReader.kvreader.RangeIterator(keyStart, deletionRowKeyEnd)
	}

	rv := &TermFieldReader{
		indexReader:        indexReader,
		count:              dictionaryRow.count,
		term:               term,
		field:              field,
		includeFreq:        includeFreq,
		includeNorm:        includeNorm,
		includeTermVectors: includeTermVectors,
		segPostingsArr:     segPostingsArr,
		deletionIter:       deletionIter,
		curSegPostings:     curSegPostings,
		keyBuf:             keyBuf,
	}

	err = rv.refreshCurDeletionRow()
	if err != nil {
		if deletionIter != nil {
			deletionIter.Close()
		}

		return nil, err
	}

	atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))

	return rv, nil
}

func (r *TermFieldReader) Count() uint64 {
	return r.count
}

func (r *TermFieldReader) Close() error {
	if r.indexReader != nil {
		atomic.AddUint64(&r.indexReader.index.stats.termSearchersFinished, uint64(1))
	}

	if r.deletionIter != nil {
		r.deletionIter.Close()
	}

	return nil
}

// --------------------------------------------------

func loadSegPostingsArr(kvreader store.KVReader, field uint16, term []byte) ([]*segPostings, error) {
	bufSize := PostingRowKeySize(term)
	if bufSize < DeletionRowKeySize {
		bufSize = DeletionRowKeySize
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
			return nil, err
		}

		it.Next()
		k, v, valid = it.Current()
		if !valid {
			return nil, fmt.Errorf("expected postingFreqNormsRow")
		}

		rowFreqNorms, err := NewPostingFreqNormsRowKV(k, v)
		if err != nil {
			return nil, err
		}
		if rowFreqNorms.segId != rowRecIds.segId {
			return nil, fmt.Errorf("mismatched segId's for postingFreqNormsRow")
		}

		it.Next()
		k, v, valid = it.Current()
		if !valid {
			return nil, fmt.Errorf("expected postingVecsRow")
		}

		rowVecs, err := NewPostingVecsRowKV(k, v)
		if err != nil {
			return nil, err
		}
		if rowVecs.segId != rowRecIds.segId {
			return nil, fmt.Errorf("mismatched segId's for postingVecsRow")
		}

		it.Next()
		k, v, valid = it.Current()

		rv = append(rv, &segPostings{
			rowRecIds:    rowRecIds,
			rowFreqNorms: rowFreqNorms,
			rowVecs:      rowVecs,
		})
	}

	return rv, nil
}

// --------------------------------------------------

func (r *TermFieldReader) Next(preAlloced *index.TermFieldDoc) (
	*index.TermFieldDoc, error) {
LOOP_SEG:
	for r.curSegPostings != nil {
		sp := r.curSegPostings
		segId := sp.rowRecIds.segId

	LOOP_REC:
		for true {
			if sp.nextRecIdx >= len(sp.rowRecIds.recIds) {
				r.nextSegPostings()
				continue LOOP_SEG
			}

			recIdx := sp.nextRecIdx
			recId := sp.rowRecIds.recIds[recIdx]

			if r.processDeletedRec(sp, segId, recId) {
				continue LOOP_REC
			}

			sp.nextRecIdx++

			// The deletionRow is nil or is > recId, so found a rec.
			return r.prepareResultRec(sp, recIdx, recId, preAlloced)
		}
	}

	return nil, nil
}

func (r *TermFieldReader) Advance(wantId index.IndexInternalID,
	preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	wantSegId := binary.LittleEndian.Uint64(wantId[:8])
	wantRecId := binary.LittleEndian.Uint64(wantId[8:])

LOOP_SEG:
	for r.curSegPostings != nil {
		sp := r.curSegPostings
		segId := sp.rowRecIds.segId

		if segId < wantSegId {
			r.nextSegPostings()
			continue LOOP_SEG
		}

	LOOP_REC:
		for true {
			if sp.nextRecIdx >= len(sp.rowRecIds.recIds) {
				r.nextSegPostings()
				continue LOOP_SEG
			}

			recIdx := sp.nextRecIdx
			recId := sp.rowRecIds.recIds[recIdx]

			if segId == wantSegId && recId < wantRecId {
				sp.nextRecIdx++
				continue LOOP_REC
			}

			if r.processDeletedRec(sp, segId, recId) {
				continue LOOP_REC
			}

			sp.nextRecIdx++

			// The deletionRow is nil or is > recId, so found a rec.
			return r.prepareResultRec(sp, recIdx, recId, preAlloced)
		}
	}

	return nil, nil
}

func (r *TermFieldReader) nextSegPostings() error {
	r.curSegPostings = nil

	r.segPostingsArr = r.segPostingsArr[1:]
	if len(r.segPostingsArr) > 0 {
		r.curSegPostings = r.segPostingsArr[0]
	}

	return r.refreshCurDeletionRow()
}

func (r *TermFieldReader) seekDeletionIter(segId, recId uint64) error {
	if r.deletionIter != nil {
		r.tmpDeletionRow.segId = recId
		r.tmpDeletionRow.recId = recId

		keyBuf := r.keyBuf[:DeletionRowKeySize]
		keyBufUsed, _ := r.tmpDeletionRow.KeyTo(keyBuf)

		r.deletionIter.Seek(keyBuf[:keyBufUsed])
	}

	return r.refreshCurDeletionRow()
}

func (r *TermFieldReader) refreshCurDeletionRow() error {
	r.curDeletionRow = nil

	if r.curSegPostings == nil || r.deletionIter == nil {
		return nil
	}

	deletionRowKey, _, valid := r.deletionIter.Current()
	if !valid {
		return nil
	}

	err := r.tmpDeletionRow.parseK(deletionRowKey)
	if err != nil {
		return err
	}

	r.curDeletionRow = &r.tmpDeletionRow

	return nil
}

func (r *TermFieldReader) processDeletedRec(sp *segPostings,
	segId uint64, recId uint64) bool {
	if r.curDeletionRow != nil {
		if r.curDeletionRow.segId < segId {
			r.seekDeletionIter(segId, recId)
			return true
		}

		if r.curDeletionRow.segId == segId {
			if r.curDeletionRow.recId < recId {
				r.seekDeletionIter(segId, recId)
				return true
			}

			if r.curDeletionRow.recId == recId {
				sp.nextRecIdx++ // The rec was deleted.
				return true
			}
		}
	}

	return false
}

func (r *TermFieldReader) prepareResultRec(sp *segPostings,
	recIdx int, recId uint64, preAlloced *index.TermFieldDoc) (
	*index.TermFieldDoc, error) {
	rv := preAlloced
	if rv == nil {
		rv = &index.TermFieldDoc{}
	}

	if cap(rv.ID) < 16 {
		rv.ID = make([]byte, 16)
	}
	rv.ID = rv.ID[:16]

	binary.LittleEndian.PutUint64(rv.ID[:8], sp.rowRecIds.segId)
	binary.LittleEndian.PutUint64(rv.ID[8:], recId)

	if r.includeFreq {
		rv.Freq = uint64(sp.rowFreqNorms.Freq(recIdx))
	}

	if r.includeNorm {
		rv.Norm = float64(sp.rowFreqNorms.Norm(recIdx))
	}

	if r.includeTermVectors {
		tvs, err := sp.rowVecs.TermFieldVectors(recIdx,
			r.indexReader.index.fieldCache, nil)
		if err != nil {
			return nil, err
		}

		rv.Vectors = tvs
	}

	return rv, nil
}
