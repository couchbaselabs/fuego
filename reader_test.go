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
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/moss"
)

func TestIndexReader(t *testing.T) {
	defer func() {
		err := DestroyTest()
		if err != nil {
			t.Fatal(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewFuego(moss.Name, stdTestConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextFieldWithAnalyzer("name", []uint64{},
		[]byte("test test test"), testAnalyzer))
	doc.AddField(document.NewTextFieldCustom("desc", []uint64{},
		[]byte("eat more rice"), document.IndexField|document.IncludeTermVectors, testAnalyzer))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// first look for a term that doesn't exist
	reader, err := indexReader.TermFieldReader([]byte("nope"), "name", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}
	count := reader.Count()
	if count != 0 {
		t.Errorf("Expected doc count to be: %d got: %d", 0, count)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	reader, err = indexReader.TermFieldReader([]byte("test"), "name", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}

	expectedCount = 2
	count = reader.Count()
	if count != expectedCount {
		t.Errorf("Expected doc count to be: %d got: %d", expectedCount, count)
	}

	var match *index.TermFieldDoc
	var actualCount uint64
	match, err = reader.Next(nil)
	for err == nil && match != nil {
		match, err = reader.Next(nil)
		if err != nil {
			t.Errorf("unexpected error reading next, err: %v", err)
		}
		actualCount++
	}
	if actualCount != count {
		t.Errorf("count was 2, but only saw %d", actualCount)
	}

	internalId, err := indexReader.InternalID("2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expectedMatch := &index.TermFieldDoc{
		ID:   internalId,
		Freq: 1,
		Norm: 0.5773502588272095,
		Vectors: []*index.TermFieldVector{
			{
				Field: "desc",
				Pos:   3,
				Start: 9,
				End:   13,
			},
		},
	}
	tfr, err := indexReader.TermFieldReader([]byte("rice"), "desc", true, true, true)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	match, err = tfr.Next(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expectedMatch, match) {
		t.Errorf("got %#v, %#v, expected %#v, %#v",
			match, match.Vectors[0], expectedMatch, expectedMatch.Vectors[0])
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now test usage of advance
	internalId, err = indexReader.InternalID("1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	reader, err = indexReader.TermFieldReader([]byte("test"), "name", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}

	match, err = reader.Advance(internalId, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match == nil {
		t.Fatalf("Expected match, got nil")
	}
	if !match.ID.Equals(internalId) {
		t.Errorf("Expected ID '1', got '%s'", match.ID)
	}

	fwdInternalId := incrementBytes(incrementBytes(append([]byte(nil), internalId...)))

	match, err = reader.Advance(fwdInternalId, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Fatalf("expected nil, got %#v", match)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now test creating a reader for a field that doesn't exist
	reader, err = indexReader.TermFieldReader([]byte("water"), "doesnotexist", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}
	count = reader.Count()
	if count != 0 {
		t.Errorf("expected count 0 for reader of non-existent field")
	}
	match, err = reader.Next(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}
	match, err = reader.Advance(internalId, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}

}

func TestIndexDocIdReader(t *testing.T) {
	defer func() {
		err := DestroyTest()
		if err != nil {
			t.Fatal(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewFuego(moss.Name, stdTestConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	doc := document.NewDocument("22")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("33")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test test test")))
	doc.AddField(document.NewTextFieldWithIndexingOptions("desc", []uint64{}, []byte("eat more rice"), document.IndexField|document.IncludeTermVectors))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	// first get all doc ids
	reader, err := indexReader.DocIDReaderAll()
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	id, err := reader.Next()
	count := uint64(0)
	for id != nil {
		count++
		id, err = reader.Next()
	}
	if count != expectedCount {
		t.Errorf("expected %d, got %d", expectedCount, count)
	}

	// try it again, but jump to the next doc this time
	reader2, err := indexReader.DocIDReaderAll()
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader2.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	iid22, _ := indexReader.InternalID("22")

	id, err = reader2.Advance(iid22)
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(iid22) {
		t.Errorf("expected to find id '22' for %q, got '%s'", iid22, id)
	}

	iid1 := incrementBytes(iid22)

	id, err = reader2.Advance(iid1)
	if err != nil {
		t.Error(err)
	}
	if id != nil {
		t.Errorf("expected to find id '', got '%q'", string(id))
	}
}

func TestCrashBadBackIndexRow(t *testing.T) {
	br, err := NewBackIndexRowKV([]byte{'b', 'a', ByteSeparator}, []byte{8, 0, 16, 0})
	if err != nil {
		t.Fatal(err)
	}
	if string(br.docID) != "a" {
		t.Fatal(err)
	}
}

func TestIndexDocIdOnlyReader(t *testing.T) {
	defer func() {
		err := DestroyTest()
		if err != nil {
			t.Fatal(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewFuego(moss.Name, stdTestConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := document.NewDocument("1")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("3")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("5")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("7")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("9")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	onlyIds := []string{"1", "5", "9"}
	reader, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	id, err := reader.Next()
	count := uint64(0)
	for id != nil {
		count++
		id, err = reader.Next()
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}

	// try it again, but jump
	reader2, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader2.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	iid5, _ := indexReader.InternalID("5")

	id, err = reader2.Advance(iid5)
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(iid5) {
		t.Errorf("expected to find id '5', got '%s'", id)
	}

	iid5[7] = 0xff // Advance really far.

	id, err = reader2.Advance(iid5)
	if err != nil {
		t.Error(err)
	}
	if id != nil {
		t.Errorf("expected to find id '', got '%q'", id)
	}

	// some keys aren't actually there
	onlyIds = []string{"0", "2", "4", "5", "6", "8", "a"}
	reader3, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader3.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	id, err = reader3.Next()
	count = uint64(0)
	for id != nil {
		count++
		id, err = reader3.Next()
	}
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}

	onlyIds = []string{"0", "1", "3", "5", "6", "9"}
	reader4x, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader4x.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	id, err = reader4x.Next()
	count = uint64(0)
	for id != nil {
		count++
		id, err = reader4x.Next()
	}
	if count != 4 {
		t.Errorf("expected 4, got %d", count)
	}

	// mix advance and next
	onlyIds = []string{"0", "1", "3", "5", "6", "9"}
	reader4, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader4.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	// first key is "9", because it was inserted last
	id, err = reader4.Next()
	if err != nil {
		t.Error(err)
	}
	iid, _ := indexReader.InternalID("9")
	if !id.Equals(iid) {
		t.Errorf("expected to find id '%q', got '%q'", iid, id)
	}

	// advancing to key we dont have gives next
	iid, _ = indexReader.InternalID("8")
	id, err = reader4.Advance(iid)
	if err != nil {
		t.Error(err)
	}
	iid, _ = indexReader.InternalID("5")
	if !id.Equals(iid) {
		t.Errorf("expected to find id '%q', got '%q'", iid, id)
	}

	// next after advance works
	id, err = reader4.Next()
	if err != nil {
		t.Error(err)
	}
	iid, _ = indexReader.InternalID("3")
	if !id.Equals(iid) {
		t.Errorf("expected to find id '%q', got '%q'", iid, id)
	}

	// advancing to key we do have works
	iid, _ = indexReader.InternalID("1")
	id, err = reader4.Advance(iid)
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(iid) {
		t.Errorf("expected to find id '%q', got '%q'", iid, id)
	}

	// advance backwards at end to a previous (younger) key
	iid, _ = indexReader.InternalID("5")
	id, err = reader4.Advance(iid)
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(nil) {
		t.Errorf("expected to find id '%q', got '%q'", nil, id)
	}

	// next after last key gives nil
	id, err = reader4.Next()
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(nil) {
		t.Errorf("expected to find id '', got '%q'", id)
	}

	// advance backwards to key that exists, but not in only set
	iid, _ = indexReader.InternalID("5")
	id, err = reader4.Advance(iid)
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(nil) {
		t.Errorf("expected to find id '', got '%q'", id)
	}

}
