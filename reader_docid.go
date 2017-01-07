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

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

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

			if !bytes.Equal(br.docID, []byte(r.only[r.onlyPos])) {
				ok := r.nextOnly()
				if !ok {
					return nil, nil
				}

				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()

				continue
			} else {
				rv = append([]byte(nil), br.docID...)
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

			rv := append([]byte(nil), br.docID...)
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

			if !bytes.Equal(br.docID, []byte(r.only[r.onlyPos])) {
				// the only key we seek'd to didn't exist
				// now look for the closest key that did exist in only
				r.onlyPos = sort.SearchStrings(r.only, string(br.docID))
				if r.onlyPos >= len(r.only) {
					// advanced to key after our last only key
					return nil, nil
				}

				// now seek to this new only key
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()
				continue
			} else {
				rv = append([]byte(nil), br.docID...)
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

			rv := append([]byte(nil), br.docID...)
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
