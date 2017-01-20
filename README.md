How's fuego different than bleve's upsidedown indexing approach?

First off, fuego's a fork of upsidedown, so it shares a lot of code.

That said, upsidedown's analysis phase returns a flat array of KVRows.
In contrast, fuego's analysis phase returns an analysis result struct,
so that later codepaths can access the resulting FieldRows,
TermFrequenceRows and StoredRows, without having to loop through an
entire flat array of KVRows.

fuego stores information following a lucene-like "segments" approach.
So, it's somewhat like the firestorm prototype, but actually goes
further in trying to use a classic postings format.

For each incoming batch, fuego does...

    assign a unique, ever-decreasing segId (uint64) which starts from MAX_UINT64,
      which allows newer seg's to show up first in an ordered key-val store.

    sort / collate the docId's ASC, into...
      []*AnalyzeAuxResult
      map[docId]*AnalyzeAuxResult

    assign the recId's based on the position of each docID in the sorted array of docID's

    fill a map[term][]TermFreqRow, where the arrays will be sorted by recId's.

    sort the map keys by term.

    use that to construct the postings.

    for each doc in the batch, also maintain any new FieldRows (global).

    add the posting rows, stored rows, docId lookup rows, and deletions from previous segments.

The format of important KV rows looks like...

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
                  // Perhaps can just fold this into the backIndex.
      docId:user1234(0x00)batch0xffffc -> recId1
      docId:user1234(0x00)batch0xffffe -> recId1

        note that an iterator can find the most recent batch for a docId
        and Next() through a docId's history quickly.

    deletions // Need this to be able to efficiently ignore obsoleted recId's in postings
      deletion:batch0xffffe-recId1 -> nil

    counts
      countBatchDeletions:batch0xffffe -> 1
      countBatchSize:batch0xffffe -> 1000

Asynchronous GC's of KV rows...

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
- create a new coallesced batch, but atomically remove the old, input batches?
- or, just write out a new kvstore / mossStore?
- put moss into a never-compact mode?
