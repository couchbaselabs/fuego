How's fuego different than bleve's upsidedown indexing approach?

First off, fuego is a fork of upsidedown, so it shares a lot of code.

That said, upsidedown's analysis phase returns a flat array of KVRows.

In contrast, fuego's analysis phase returns an analysis result struct
(AnalysisAuxResult), so that later codepaths can access the resulting
rows (FieldRows, TermFrequencyRows, StoredRows) already grouped by
type, without having to further loop through an entire flat array of
KVRows.

fuego also stores information following a lucene-like "segments"
approach, along with a classic postings format.

To process each incoming batch of documents, fuego performs the
following (in batch.go)...

    assigns a unique, ever _decreasing_ segId (uint64) for the batch.
      That is, segId's start from MAX_UINT64, which allows newer seg's
      to show up earlier in a key-val store ordered by key.

    collates the analysis results into...
      var batchEntriesMap map[docID]*batchEntry
      var batchEntriesArr []*batchEntry

    assigns recId's based on the (1-based) position of
      each batchEntry in the batchEntriesArr.

    a batchEntry is a struct that associates an AnalysisAuxResult
      with its unique recId.

    fills a map[fieldTerm][]TermFrequencyRow,
      where the []TermFrequencyRow arrays will be ordered
      by increasing recId.

    use that map[fieldTerm][]TermFrequencyRow to construct
      the postings per fieldTerm.

    for each doc in the batch, also maintain any new FieldRows (global).

    add the posting rows, stored rows, docID lookup rows,
      and deletions from previous segments.

We keep most of the KV row types from upsidedown, with these changes...

- We also add the segId:recId info to the value of each BackIndexRow.

- We also no longer persist the TermFrequencyRow's -- the
  TermFrequencyRow's are still used, but only as in-memory-only data
  structures from the output of analysis.

Also, additional, persisted KV rows introduced by fuego would be...

    Notes on syntax for the following...
      The colon (':') character represents concatenation.
      A tag-like <type> represents a single byte type code.
      A fieldId is a fixed length uint16.
      A 'lazy' is an example term, of variable length.
      A 0xff is a ByteSeparator (length 1 byte).
      A segId is a fixed length uint64 (not varint encoded).
      A recId is a fixed length uint64 (not varint encoded).
      A docID is a variable length []byte for the document id.

    postings
      <postings>:fieldId:'lazy':0xff:segId:<recIds> -> [recId1, recId2, recId33]
      <postings>:fieldId:'lazy':0xff:segId:<fieldNorms> -> [0, 1, 2]
      <postings>:fieldId:'lazy':0xff:segId:>positions> -> [[], [], []]

        Note that an iterator can Next() through all
          of the postings related information quickly.

    id lookups from internalId to externalId
      <id>:segId:recId -> docID

      Lookups from externalId to internalId are handled via
        backIndexRow lookups, which was enhanced to also track the
        current segId:recId info as part of each backIndexRow.

      The id lookup rows are deleted synchronously with each batch,
        as part of the batch processing's mergeOldAndNew().

      A missing (already deleted) id lookup row for a segId:recId
        means that the recId should be ignored in the postings.

    deletions
      <deletion>:segId:recId -> (nil / 0 byte value)

    counts
      <countSegDeletions>:segId -> 1
      <countSegSize>:segId -> 1000

    summary row (a singleton row) -> lastUsedSegId

      Similar to the version row, there's only one summary row, which
      tracks the lastUsedSegId, and perhaps other future index-global
      information.

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
