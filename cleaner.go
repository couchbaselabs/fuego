//  Copyright (c) 2017 Couchbase, Inc.
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
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/index/store"
)

var MinSegDirtiness = int64(4) // TODO: Pick better MinSegDirtiness.

// Max number of segs to clean concurrently.
var MaxConcurrentSegsToClean = 4 // TODO: Pick better MaxConcurrentSegsToClean.

// The segDirtinessIncoming tells us which seg's recently had changes.
func (udc *Fuego) Cleaner(segDirtinessIncoming map[uint64]int64) error {
	udc.writeMutex.Lock()

	// Incorporate the latest dirtiness info.
	for segId, count := range segDirtinessIncoming {
		udc.segDirtiness[segId] += count
	}

	err := udc.CleanerLOCKED()

	udc.writeMutex.Unlock()

	return err
}

// The segDirtinessIncoming tells us which seg's recently had changes.
func (udc *Fuego) CleanerLOCKED() error {
	kvreader, err := udc.store.Reader()
	if err != nil {
		return err
	}
	defer kvreader.Close()

	fieldIds, err := udc.LoadFieldIds(kvreader)
	if err != nil {
		return err
	}

	onlySegIds, err := udc.FindSegsToCleanLOCKED()
	if err != nil {
		return err
	}

	udc.summaryRow.LastUsedSegId = udc.summaryRow.LastUsedSegId - 1

	currSegId := udc.summaryRow.LastUsedSegId

	addRowsAll, updateRowsAll, deleteRowsAll, err :=
		udc.CleanFieldsLOCKED(kvreader, fieldIds, onlySegIds, currSegId)
	if err != nil {
		return err
	}

	updateRowsAll = append(updateRowsAll, []KVRow{NewSummaryRow(currSegId)})

	// start a writer for this batch
	kvwriter, err := udc.store.Writer()
	if err != nil {
		return err
	}
	defer kvwriter.Close()

	return udc.batchRows(kvwriter, addRowsAll, updateRowsAll, deleteRowsAll, nil)
}

func (udc *Fuego) FindSegsToCleanLOCKED() (map[uint64]struct{}, error) {
	numSegs := len(udc.segDirtiness)

	// Group segIds by those that are dirty vs just dusty.
	dirtySegIds := make(uint64s, 0, numSegs)
	dustySegIds := make(uint64s, 0, numSegs)

	for segId, dirtiness := range udc.segDirtiness {
		if dirtiness > MinSegDirtiness {
			dirtySegIds = append(dirtySegIds, segId)
		} else {
			dustySegIds = append(dustySegIds, segId)
		}
	}

	sort.Sort(dirtySegIds)
	sort.Sort(dustySegIds)

	// Concatenate so dirty segId's come before the dusty segId's.
	candidateSegIds := append(dirtySegIds, dustySegIds...)

	// Don't clean too many segs at once.
	//
	// TODO: Have a better, more dynamic policy than this.
	//
	if len(candidateSegIds) > MaxConcurrentSegsToClean {
		candidateSegIds = candidateSegIds[:MaxConcurrentSegsToClean]
	}

	onlySegIds := map[uint64]struct{}{}
	for _, segId := range candidateSegIds {
		onlySegIds[segId] = struct{}{}
	}

	return onlySegIds, nil
}

func (udc *Fuego) CleanFieldsLOCKED(kvreader store.KVReader, fieldIds []uint16,
	onlySegIds map[uint64]struct{}, currSegId uint64) (
	addRowsAll [][]KVRow,
	updateRowsAll [][]KVRow,
	deleteRowsAll [][]KVRow,
	err error) {
	buf := GetRowBuffer()

	segVisitor := &segVisitor{
		onlySegIds: onlySegIds,
		tmpSegPostings: segPostings{
			rowRecIds:    &PostingRecIdsRow{},
			rowFreqNorms: &PostingFreqNormsRow{},
			rowVecs:      &PostingVecsRow{},
		},
		buf: buf,
	}

	var currFieldId uint16
	var currTerm []byte

	var nextRecId uint64 = uint64(1)

	var recIdsNewSeg [][]uint64
	var freqNormsNewSeg [][]uint32
	var vectorsNewSeg [][][]uint32

	var recIdsCur []uint64
	var freqNormsCur []uint32
	var vectorsCur [][]uint32

	// If the fieldId or term has changed, then flushFieldTerm()
	// appends the collected "NewSeg" info into a new triplet of
	// posting rows in addRowsAll.
	flushFieldTerm := func(fieldId uint16, term []byte) {
		if currFieldId != fieldId || !bytes.Equal(currTerm, term) {
			if nextRecId > 1 {
				numRecs := int(nextRecId)

				recIdsFlat := make([]uint64, 0, numRecs)
				for _, recIds := range recIdsNewSeg {
					recIdsFlat = append(recIdsFlat, recIds...)
				}
				recIdsNewSeg = nil

				freqNormsFlat := make([]uint32, 0, numRecs * 2)
				for _, freqNorms := range freqNormsNewSeg {
					freqNormsFlat = append(freqNormsFlat, freqNorms...)
				}
				freqNormsNewSeg = nil

				lenVectors := 0
				for _, vectorsFromOldSeg := range vectorsNewSeg {
					for _, vectors := range vectorsFromOldSeg {
						lenVectors += len(vectors)
					}
				}

				vectorsEncoded := make([]uint32, 1 + numRecs + lenVectors)

				vectorsEncoded[0] = uint32(numRecs)

				partOffsets := vectorsEncoded[1:]
				parts := vectorsEncoded[1+numRecs:]
				partsUsed := 0

				for i, vectorsFromOldSeg := range vectorsNewSeg {
					for _, vectors := range vectorsFromOldSeg {
						partOffsets[i] = uint32(partsUsed)
						partsUsed += copy(parts[partsUsed:], vectors)
					}
				}
				vectorsNewSeg = nil

				addRowsAll = append(addRowsAll, []KVRow{
					NewPostingRecIdsRow(
						currFieldId, currTerm, currSegId, recIdsFlat),
					NewPostingFreqNormsRow(
						currFieldId, currTerm, currSegId, freqNormsFlat),
					NewPostingVecsRow(
						currFieldId, currTerm, currSegId, vectorsEncoded),
				})
			}

			recIdsCur = nil
			freqNormsCur = nil
			vectorsCur = nil

			nextRecId = 1
		}

		currFieldId = fieldId
		currTerm = term
	}

	// When visitor sees a field/term/segId/recId, append it to the
	// "Cur" info.
	//
	// When visitor sees a field/term/segId ended, append the
	// collected "Cur" info to the "NewSeg" info.
	//
	// When visitor sees a field/term ended, call flushFieldTerm()
	visitor := func(fieldId uint16, term []byte,
		sp *segPostings, segId uint64,
		recIdx int, recId uint64, alive bool) (
		keepGoing bool, err error) {
		if recIdx >= 0 {
			if alive {
				// Retrieve the id lookup row.
				idRow := NewIdRow(segId, recId, nil)
				if len(buf) < idRow.KeySize() {
					buf = make([]byte, idRow.KeySize())
				}
				bufUsed, _ := idRow.KeyTo(buf)

				docIDBytes, err := kvreader.Get(buf[:bufUsed])
				if err != nil {
					return false, err
				}

				if len(docIDBytes) > 0 {
					deleteRowsAll = append(deleteRowsAll, []KVRow{idRow})

					backIndexRow, err :=
						backIndexRowForDocID(kvreader, docIDBytes, nil)
					if err != nil {
						return false, err
					}

					if backIndexRow != nil {
						backIndexRow.segId = currSegId
						backIndexRow.recId = nextRecId

						updateRowsAll = append(updateRowsAll,
							[]KVRow{backIndexRow})

						addRowsAll = append(addRowsAll,
							[]KVRow{NewIdRow(currSegId, nextRecId, docIDBytes)})

						recIdsCur = append(recIdsCur, nextRecId)

						freqNormsCur = append(freqNormsCur,
							sp.rowFreqNorms.Freq(recIdx),
							sp.rowFreqNorms.NormEncoded(recIdx))

						vectorsEncoded, err :=
							sp.rowVecs.TermFieldVectorsEncoded(recIdx)
						if err != nil {
							return false, err
						}

						vectorsCur = append(vectorsCur, vectorsEncoded)

						nextRecId += 1
					}
				}
			} else {
				deleteRowsAll = append(deleteRowsAll,
					[]KVRow{NewDeletionRow(segId, recId)})
			}
		} else {
			if alive { // Started a new field/term/segId.
				flushFieldTerm(fieldId, term)

				recIdsCur = make([]uint64, 0, len(sp.rowRecIds.recIds))
				freqNormsCur = make([]uint32, 0, len(sp.rowFreqNorms.freqNorms))
				vectorsCur = make([][]uint32, 0, len(sp.rowRecIds.recIds))

				deleteRowsAll = append(deleteRowsAll, []KVRow{
					NewPostingRecIdsRow(fieldId, term, segId, nil),
					NewPostingFreqNormsRow(fieldId, term, segId, nil),
					NewPostingVecsRowFromVectors(fieldId, term, segId, nil),
				})
			} else { // Ended a new field/term/segId.
				if len(recIdsCur) > 0 {
					recIdsNewSeg = append(recIdsNewSeg, recIdsCur)
					freqNormsNewSeg = append(freqNormsNewSeg, freqNormsCur)
					vectorsNewSeg = append(vectorsNewSeg, vectorsCur)

					recIdsCur = nil
					freqNormsCur = nil
					vectorsCur = nil
				}
			}
		}

		return true, nil
	}

	for _, fieldId := range fieldIds {
		err = segVisitor.Visit(kvreader, fieldId, visitor)
		if err != nil {
			break
		}

		flushFieldTerm(0xffff, nil)
	}

	if len(recIdsCur) > 0 {
		recIdsNewSeg = append(recIdsNewSeg, recIdsCur)
		freqNormsNewSeg = append(freqNormsNewSeg, freqNormsCur)
		vectorsNewSeg = append(vectorsNewSeg, vectorsCur)
	}

	segVisitor.Reset()

	PutRowBuffer(buf)

	return addRowsAll, updateRowsAll, deleteRowsAll, err
}

// --------------------------------------------------------

// A segVisitor visits the term posting rows for a given fieldId, and
// originally came from the TermFieldReader's codepaths for iterating
// through postings.
type segVisitor struct {
	onlySegIds map[uint64]struct{}

	postingsIter store.KVIterator // Iterates through postings rows.
	deletionIter store.KVIterator // Iterates through deletion rows.

	curSegPostings *segPostings
	curDeletionRow *DeletionRow

	tmpSegPostings segPostings
	tmpDeletionRow DeletionRow

	buf []byte
}

type segVisitorFunc func(fieldId uint16, term []byte,
	sp *segPostings, segId uint64,
	recIdx int, recId uint64, alive bool) (
	keepGoing bool, err error)

func (c *segVisitor) Reset() {
	if c.postingsIter != nil {
		c.postingsIter.Close()
		c.postingsIter = nil
	}

	if c.deletionIter != nil {
		c.deletionIter.Close()
		c.deletionIter = nil
	}

	c.curSegPostings = nil
	c.curDeletionRow = nil
}

func (c *segVisitor) Visit(kvreader store.KVReader,
	fieldId uint16, visitor segVisitorFunc) error {
	c.Reset()

	err := c.StartIterator(kvreader, fieldId)
	if err != nil {
		return err
	}

	for c.curSegPostings != nil {
		sp := c.curSegPostings
		segId := sp.rowRecIds.segId

		if _, wanted := c.onlySegIds[segId]; wanted {
			term := sp.rowRecIds.term

			visitor(fieldId, term, sp, segId, -1, 0, true)

		LOOP_REC:
			for sp.nextRecIdx < len(sp.rowRecIds.recIds) {
				recIdx := sp.nextRecIdx
				recId := sp.rowRecIds.recIds[recIdx]

				if c.processDeletedRec(sp, segId, recId) {
					keepGoing, err :=
						visitor(fieldId, term, sp, segId, recIdx, recId, false)
					if !keepGoing || err != nil {
						return err
					}

					continue LOOP_REC
				}

				sp.nextRecIdx++

				// The deletionRow is nil or is > recId, so found a rec.
				keepGoing, err :=
					visitor(fieldId, term, sp, segId, recIdx, recId, true)
				if !keepGoing || err != nil {
					return err
				}
			}

			visitor(fieldId, term, sp, segId, -1, 0, false)
		}

		c.nextSegPostings()
	}

	return nil
}

// --------------------------------------------------------

func (c *segVisitor) StartIterator(kvreader store.KVReader,
	field uint16) error {
	if len(c.buf) < PostingRowKeyFieldSize {
		c.buf = make([]byte, PostingRowKeyFieldSize)
	}
	bufUsed := PostingRowKeyFieldPrefix(field, c.buf)

	c.postingsIter = kvreader.PrefixIterator(c.buf[:bufUsed])

	err := c.nextSegPostings()
	if err != nil || c.curSegPostings == nil {
		return err
	}

	if len(c.buf) < DeletionRowKeySize {
		c.buf = make([]byte, DeletionRowKeySize)
	}
	bufUsed = DeletionRowKeyPrefix(c.curSegPostings.rowRecIds.segId, c.buf)

	c.deletionIter = kvreader.RangeIterator(c.buf[:bufUsed], deletionRowKeyEnd)

	return nil
}

// --------------------------------------------------------

func (c *segVisitor) nextSegPostings() error {
	c.curSegPostings = nil

	if c.postingsIter == nil {
		return nil
	}

	k, v, valid := c.postingsIter.Current()
	if !valid {
		return nil
	}
	rowRecIds := c.tmpSegPostings.rowRecIds
	err := rowRecIds.parseK(k)
	if err != nil {
		return err
	}
	err = rowRecIds.parseV(v)
	if err != nil {
		return err
	}

	c.postingsIter.Next()
	k, v, valid = c.postingsIter.Current()
	if !valid {
		return fmt.Errorf("expected postingFreqNormsRow")
	}
	rowFreqNorms := c.tmpSegPostings.rowFreqNorms
	err = rowFreqNorms.parseK(k)
	if err != nil {
		return err
	}
	if rowFreqNorms.segId != rowRecIds.segId {
		return fmt.Errorf("mismatched segId's for postingFreqNormsRow")
	}
	err = rowFreqNorms.parseV(v)
	if err != nil {
		return err
	}

	c.postingsIter.Next()
	k, v, valid = c.postingsIter.Current()
	if !valid {
		return fmt.Errorf("expected postingVecsRow")
	}
	rowVecs := c.tmpSegPostings.rowVecs
	err = rowVecs.parseK(k)
	if err != nil {
		return err
	}
	if rowVecs.segId != rowRecIds.segId {
		return fmt.Errorf("mismatched segId's for postingVecsRow")
	}
	err = rowVecs.parseV(v)
	if err != nil {
		return err
	}

	c.postingsIter.Next()

	c.tmpSegPostings.nextRecIdx = 0

	c.curSegPostings = &c.tmpSegPostings

	return nil
}

func (c *segVisitor) processDeletedRec(sp *segPostings,
	segId uint64, recId uint64) bool {
	if c.curDeletionRow == nil {
		return false
	}

	if c.curDeletionRow.segId < segId {
		c.seekDeletionIter(segId, recId)
		return true
	}

	if c.curDeletionRow.segId == segId {
		if c.curDeletionRow.recId < recId {
			c.seekDeletionIter(segId, recId)
			return true
		}

		if c.curDeletionRow.recId == recId {
			sp.nextRecIdx++ // The rec was deleted.
			return true
		}
	}

	return false
}

func (c *segVisitor) seekDeletionIter(segId, recId uint64) error {
	if c.deletionIter != nil {
		c.tmpDeletionRow.segId = recId
		c.tmpDeletionRow.recId = recId

		buf := c.buf[:DeletionRowKeySize]
		bufUsed, _ := c.tmpDeletionRow.KeyTo(buf)

		c.deletionIter.Seek(buf[:bufUsed])
	}

	return c.refreshCurDeletionRow()
}

func (c *segVisitor) refreshCurDeletionRow() error {
	c.curDeletionRow = nil

	if c.deletionIter == nil {
		return nil
	}

	deletionRowKey, _, valid := c.deletionIter.Current()
	if !valid {
		return nil
	}

	err := c.tmpDeletionRow.parseK(deletionRowKey)
	if err != nil {
		return err
	}

	c.curDeletionRow = &c.tmpDeletionRow

	return nil
}
