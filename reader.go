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
	"sort"
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

type DocIDReader struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	only        []string
	onlyPos     int
	onlyMode    bool
}

func newDocIDReader(indexReader *IndexReader) (*DocIDReader, error) {
	startBytes := []byte{0x0}
	endBytes := []byte{0xff}

	bisr := NewBackIndexRow(startBytes, nil, nil)
	bier := NewBackIndexRow(endBytes, nil, nil)
	it := indexReader.kvreader.RangeIterator(bisr.Key(), bier.Key())

	return &DocIDReader{
		indexReader: indexReader,
		iterator:    it,
	}, nil
}

func newDocIDReaderOnly(indexReader *IndexReader, ids []string) (*DocIDReader, error) {
	// ensure ids are sorted
	sort.Strings(ids)

	startBytes := []byte{0x0}
	if len(ids) > 0 {
		startBytes = []byte(ids[0])
	}

	endBytes := []byte{0xff}
	if len(ids) > 0 {
		endBytes = incrementBytes([]byte(ids[len(ids)-1]))
	}

	bisr := NewBackIndexRow(startBytes, nil, nil)
	bier := NewBackIndexRow(endBytes, nil, nil)
	it := indexReader.kvreader.RangeIterator(bisr.Key(), bier.Key())

	return &DocIDReader{
		indexReader: indexReader,
		iterator:    it,
		only:        ids,
		onlyMode:    true,
	}, nil
}

func (r *DocIDReader) Next() (index.IndexInternalID, error) {
	key, val, valid := r.iterator.Current()

	if r.onlyMode {
		var rv index.IndexInternalID
		for valid && r.onlyPos < len(r.only) {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}

			if !bytes.Equal(br.doc, []byte(r.only[r.onlyPos])) {
				ok := r.nextOnly()
				if !ok {
					return nil, nil
				}

				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()

				continue
			} else {
				rv = append([]byte(nil), br.doc...)
				break
			}
		}

		if valid && r.onlyPos < len(r.only) {
			ok := r.nextOnly()
			if ok {
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
			}

			return rv, nil
		}

	} else {
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}

			rv := append([]byte(nil), br.doc...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *DocIDReader) Advance(docID index.IndexInternalID) (index.IndexInternalID, error) {
	if r.onlyMode {
		r.onlyPos = sort.SearchStrings(r.only, string(docID))
		if r.onlyPos >= len(r.only) {
			// advanced to key after our last only key
			return nil, nil
		}

		r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
		key, val, valid := r.iterator.Current()

		var rv index.IndexInternalID
		for valid && r.onlyPos < len(r.only) {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}

			if !bytes.Equal(br.doc, []byte(r.only[r.onlyPos])) {
				// the only key we seek'd to didn't exist
				// now look for the closest key that did exist in only
				r.onlyPos = sort.SearchStrings(r.only, string(br.doc))
				if r.onlyPos >= len(r.only) {
					// advanced to key after our last only key
					return nil, nil
				}

				// now seek to this new only key
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()
				continue
			} else {
				rv = append([]byte(nil), br.doc...)
				break
			}
		}
		if valid && r.onlyPos < len(r.only) {
			ok := r.nextOnly()
			if ok {
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
			}

			return rv, nil
		}
	} else {
		bir := NewBackIndexRow(docID, nil, nil)
		r.iterator.Seek(bir.Key())

		key, val, valid := r.iterator.Current()
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}

			rv := append([]byte(nil), br.doc...)
			r.iterator.Next()

			return rv, nil
		}
	}

	return nil, nil
}

func (r *DocIDReader) Close() error {
	return r.iterator.Close()
}

// move the r.only pos forward one, skipping duplicates
// return true if there is more data, or false if we got to the end of the list
func (r *DocIDReader) nextOnly() bool {
	// advance 1 position, until we see a different key
	// it's already sorted, so this skips duplicates
	start := r.onlyPos

	r.onlyPos++
	for r.onlyPos < len(r.only) && r.only[r.onlyPos] == r.only[start] {
		start = r.onlyPos
		r.onlyPos++
	}

	// indicate if we got to the end of the list
	return r.onlyPos < len(r.only)
}

func (udc *Fuego) termFieldVectorsFromTermVectors(in []*TermVector) []*index.TermFieldVector {
	if len(in) <= 0 {
		return nil
	}

	rv := make([]*index.TermFieldVector, len(in))

	for i, tv := range in {
		fieldName := udc.fieldCache.FieldIndexed(tv.field)
		tfv := index.TermFieldVector{
			Field:          fieldName,
			ArrayPositions: tv.arrayPositions,
			Pos:            tv.pos,
			Start:          tv.start,
			End:            tv.end,
		}
		rv[i] = &tfv
	}

	return rv
}
