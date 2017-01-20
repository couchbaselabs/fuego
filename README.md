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
      <postings>:field4:'lazy'-segId:<recIds> -> [recId1, recId2, recId3]
      <postings>:field4:'lazy'-segId:<fieldNorms> -> [0, 1, 2]
      <postings>:field4:'lazy'-segId:>positions> -> [[], [], []]

        note that an iterator can Next() through all of the postings columns quickly.

    deletions // Need this to be able to efficiently ignore obsoleted recId's in postings
      <deletion>:segId-recId1 -> nil

    counts
      countSegDeletions:segId -> 1
      countSegSize:segId -> 1000

    also, add the segId:recId to the value of each BackIndexRow.

Asynchronous GC's of KV rows...

    that represent outdated seg's and records.

    scan the counts to see which seg's might have the most garbage?

    for example, doc "user-1234" in segIdffffe is outdated
      due to the newer, younger update of "user-1234" in segIdffffc, so...

      need to delete...
        "<deletion>:segIdffffe-recId1"

        and go through all the postings on any field with segIdffffe, recId1
          maybe just use a postings iterator and a recId set filter?

need a way to coallesce batches?
- create a new coallesced batch, but atomically remove the old, input batches?
- or, just write out a new kvstore / mossStore?
- put moss into a never-compact mode?
