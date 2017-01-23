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

type IdReader struct {
	iterator store.KVIterator // Iterates through IdRow's.
	tmpKey   []byte
}

type IdSetReader struct {
	ids internalIds // Iterates through this fixed set of internalIds.
	pos int
}

// ------------------------------------------------------

func newIdReader(indexReader *IndexReader) (*IdReader, error) {
	tmpKey := make([]byte, IdRowKeySize)
	tmpKey[0] = IdRowPrefix[0]

	return &IdReader{
		iterator: indexReader.kvreader.PrefixIterator(IdRowPrefix),
		tmpKey:   tmpKey,
	}, nil
}

func newIdSetReaderFromDocIDs(indexReader *IndexReader, docIDs []string) (
	*IdSetReader, error) {
	ids := make(internalIds, 0, len(docIDs))

	for _, docID := range docIDs {
		internalId, err := indexReader.InternalID(docID)
		if err != nil {
			return nil, err
		}
		if internalId != nil {
			ids = append(ids, internalId)
		}
	}

	sort.Sort(ids)

	return &IdSetReader{ids: ids}, nil
}

// ------------------------------------------------------

func (r *IdReader) Next() (index.IndexInternalID, error) {
	key, _, valid := r.iterator.Current()
	if valid {
		rv := make([]byte, 16)
		copy(rv, key[1:]) // Strip off 1st byte type prefix.
		r.iterator.Next()
		return rv, nil
	}

	return nil, nil
}

func (r *IdReader) Advance(wantInternalId index.IndexInternalID) (
	index.IndexInternalID, error) {
	copy(r.tmpKey[1:], wantInternalId)
	r.iterator.Seek(r.tmpKey)
	return r.Next()
}

func (r *IdReader) Close() error {
	return r.iterator.Close()
}

// ------------------------------------------------------

func (r *IdSetReader) Next() (index.IndexInternalID, error) {
	if r.pos >= len(r.ids) {
		return nil, nil
	}

	rv := r.ids[r.pos]
	r.pos++

	return rv, nil
}

func (r *IdSetReader) Advance(wantInternalId index.IndexInternalID) (
	index.IndexInternalID, error) {
	for r.pos < len(r.ids) {
		if bytes.Compare(r.ids[r.pos], wantInternalId) >= 0 {
			return r.Next()
		}

		r.pos++
	}

	return nil, nil
}

func (r *IdSetReader) Close() error {
	return nil
}
