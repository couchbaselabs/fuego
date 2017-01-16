for each batch...

  assign a unique, ever-decreasing segId (uint64) which starts from MAX_UINT64,
    which allows newer batches to show up first in an ordered key-val store.

  sort the docId's ASC
    []*AnalyzeAuxResult
    map[docId]*AnalyzeAuxResult

  assign the recId's based on the position of a docID position in the sorted array of docID's

  sort the TermFreqRows in each AnalyzeAuxResult by field

  for each doc in the batch...

    maintain any new FieldRows (global)

    increment a recId, starting from 1, so "segId:recId" is unique

    postings
      postings:field4:'lazy'-segId:recIds -> [recId1, recId2, recId3]
      postings:field4:'lazy'-segId:fieldNorms -> [0, 1, 2]
      postings:field4:'lazy'-segId:positions -> [[], [], []]

        note that an iterator can Next() through all of the postings columns quickly.

    stored fields
      stored:batch0xffffc-recId1:field0:arrayPositions -> "typ:user1234" // The docId.
      stored:batch0xffffc-recId1:field1:arrayPositions -> "typ:john@email.com"

        note that an iterator can Next() through all of the stored fields
        of a record quickly.

    docId lookups // Newer segId's appear first since segId's grow downwards.
      docId:user1234(0x00)batch0xffffc -> recId1
      docId:user1234(0x00)batch0xffffe -> recId1

        note that an iterator can find the most recent batch for a docId
        and Next() through a docId's history quickly.

    deletions // Need this to be able to efficiently ignore obsoleted recId's in postings
      deletion:batch0xffffe-recId1 -> nil

    counts
      countBatchDeletions:batch0xffffe -> 1
      countBatchSize:batch0xffffe -> 1000

asynchronous GC's of key-val's
  that represent outdated batches and records.

    scan the counts to see which batches might have the most garbage?

    for example, "docId:user1234(0x00)batch0xffffe" is outdated
      due to the newer, younger "docId:user1234(0x00)batch0xffffc" record...

      so also need to delete...
        "deletion:batch0xffffe-recId1"

        and go through all the postings on any field with batch0xffffe, recId1
          maybe just use a postings iterator and a recId set filter?

        and any other stored fields with batch0xffffe-recId1

      we use "docId:" record instead of "deletion:" record to look
        for outdated information as it has more information.
        or, perhaps scan the deletion:segId-recId records,
        to focus on one batch at a time?

need a way to coallesce batches?
  create a new coallesced batch, but atomically remove the old, input batches?
  or, just write out a new kvstore / mossStore?
  put moss into a never-compact mode?
