for each batch...

  assign a unique, ever-decreasing batchId (uint64) which starts from MAX_UINT64,
    which allows newer batches to show up first in an ordered key-val store.

  SET new FieldRows (global)

  for each doc in the batch...

    increment a recordId, starting from 1, so "batchId:recordId" is unique

    postings
      postings:field4:'lazy'-batchId:recordIds -> [recordId1, recordId2, recordId3]
      postings:field4:'lazy'-batchId:fieldNorms -> [0, 1, 2]
      postings:field4:'lazy'-batchId:termVectors -> [[], [], []]

        note that an iterator can Next() through all of the postings columns quickly.

    stored fields
      stored:batch0xffffc-recordId1:field0 -> "user1234" // The docId.
      stored:batch0xffffc-recordId1:field1 -> "john@email.com"

        note that an iterator can Next() through all of the stored fields
        of a record quickly.

    docId lookups // Newer batchId's appear first since batchId's grow downwards.
      docId:user1234-batch0xffffc -> recordId1
      docId:user1234-batch0xffffe -> recordId1

        note that an iterator can find the most recent batch for a docId
        and Next() through a docId's history quickly.

    deletions // Need this to be able to efficiently ignore obsoleted recordId's in postings
      deletion:batch0xffffe-recordId1 -> nil

    counts
      countDeletions:batch0xffffe -> 1
      countSize:batch0xffffe -> 1000

asynchronous GC's of key-val's
  that represent outdated batches and records.

    scan the counts to see which batches might have the most garbage?

    for example, "docId:user1234-batch0xffffe" is outdated
      due to the newer, younger "docId:user1234-batch0xffffc" record...

      so also need to delete...
        "deletion:batch0xffffe-recordId1"

        and go through all the postings on any field with batch0xffffe, recordId1
          maybe just use a postings iterator and a recordId set filter?

        and any other stored fields with batch0xffffe-recordId1

      we use "docId:" record instead of "deletion:" record to look
        for outdated information as it has more information.
        or, perhaps scan the deletion:batchId-recordId records,
        to focus on one batch at a time?

need a way to coallesce batches?
  create a new coallesced batch, but atomically remove the old, input batches?
  or, just write out a new kvstore / mossStore?
  put moss into a never-compact mode?
