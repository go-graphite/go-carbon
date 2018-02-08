// Package trigram is a simple trigram index
package trigram

import (
	"sort"
)

// T is a trigram
type T uint32

func (t T) String() string {
	b := [3]byte{byte(t >> 16), byte(t >> 8), byte(t)}
	return string(b[:])
}

// DocID is a document ID
type DocID uint32

// Index is a trigram index
type Index map[T][]DocID

// a special (and invalid) trigram that holds all the document IDs
const TAllDocIDs T = 0xFFFFFFFF

// Extract returns a list of all the unique trigrams in s
func Extract(s string, trigrams []T) []T {

	for i := 0; i <= len(s)-3; i++ {
		t := T(uint32(s[i])<<16 | uint32(s[i+1])<<8 | uint32(s[i+2]))
		trigrams = appendIfUnique(trigrams, t)
	}

	return trigrams
}

// ExtractAll returns a list of all the trigrams in s
func ExtractAll(s string, trigrams []T) []T {

	for i := 0; i <= len(s)-3; i++ {
		t := T(uint32(s[i])<<16 | uint32(s[i+1])<<8 | uint32(s[i+2]))
		trigrams = append(trigrams, t)
	}

	return trigrams
}

func appendIfUnique(t []T, n T) []T {
	for _, v := range t {
		if v == n {
			return t
		}
	}

	return append(t, n)
}

// NewIndex returns an index for the strings in docs
func NewIndex(docs []string) Index {

	idx := make(Index)

	var allDocIDs []DocID

	var trigrams []T

	for id, d := range docs {
		ts := ExtractAll(d, trigrams)
		docid := DocID(id)
		allDocIDs = append(allDocIDs, docid)
		for _, t := range ts {
			idxt := idx[t]
			l := len(idxt)
			if l == 0 || idxt[l-1] != docid {
				idx[t] = append(idxt, docid)
			}
		}
		trigrams = trigrams[:0]
	}

	idx[TAllDocIDs] = allDocIDs

	return idx
}

// Add adds a new string to the search index
func (idx Index) Add(s string) DocID {
	id := DocID(len(idx[TAllDocIDs]))
	idx.Insert(s, id)
	return id
}

// AddTrigrams adds a set of trigrams to the search index
func (idx Index) AddTrigrams(ts []T) DocID {
	id := DocID(len(idx[TAllDocIDs]))
	idx.InsertTrigrams(ts, id)
	return id
}

// Insert adds a string with a given document ID
func (idx Index) Insert(s string, id DocID) {
	ts := ExtractAll(s, nil)
	idx.InsertTrigrams(ts, id)
}

// InsertTrigrams adds a set of trigrams with a given document ID
func (idx Index) InsertTrigrams(ts []T, id DocID) {
	for _, t := range ts {
		idxt, ok := idx[t]
		// this trigram post list has been pruned. we must keep it empty
		if ok && idxt == nil {
			continue
		}
		l := len(idxt)
		if l == 0 || idxt[l-1] != id {
			idx[t] = append(idxt, id)
		}
	}

	idx[TAllDocIDs] = append(idx[TAllDocIDs], id)
}

// Delete removes a document from the index
func (idx Index) Delete(s string, id DocID) {
	ts := ExtractAll(s, nil)
	for _, t := range ts {
		ids := idx[t]
		if ids == nil {
			continue
		}

		if len(ids) == 1 && ids[0] == id {
			delete(idx, t)
			continue
		}

		i := sort.Search(len(ids), func(i int) bool { return ids[i] >= id })

		if i != -1 && i < len(ids) && ids[i] == id {
			copy(ids[i:], ids[i+1:])
			idx[t] = ids[:len(ids)-1]
		}
	}
}

// for sorting
type docList []DocID

func (d docList) Len() int           { return len(d) }
func (d docList) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d docList) Less(i, j int) bool { return d[i] < d[j] }

// Sort ensures all the document IDs are in order.
func (idx Index) Sort() {
	for _, v := range idx {
		dl := docList(v)
		if !sort.IsSorted(dl) {
			sort.Sort(dl)
		}
	}
}

// Prune removes all trigrams that are present in more than the specified percentage of the documents.
func (idx Index) Prune(pct float64) int {

	maxDocs := int(pct * float64(len(idx[TAllDocIDs])))

	var pruned int

	for k, v := range idx {
		if k != TAllDocIDs && len(v) > maxDocs {
			pruned++
			idx[k] = nil
		}
	}

	return pruned
}

// Query returns a list of document IDs that match the trigrams in the query s
func (idx Index) Query(s string) []DocID {
	ts := Extract(s, nil)
	return idx.QueryTrigrams(ts)
}

type tfList struct {
	tri  []T
	freq []int
}

func (tf tfList) Len() int { return len(tf.tri) }
func (tf tfList) Swap(i, j int) {
	tf.tri[i], tf.tri[j] = tf.tri[j], tf.tri[i]
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
}
func (tf tfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

// QueryTrigrams returns a list of document IDs that match the trigram set ts
func (idx Index) QueryTrigrams(ts []T) []DocID {

	if len(ts) == 0 {
		return idx[TAllDocIDs]
	}

	var freq []int

	for _, t := range ts {
		d, ok := idx[t]
		if !ok {
			return nil
		}
		freq = append(freq, len(d))
	}

	sort.Sort(tfList{ts, freq})

	var nonzero int
	for nonzero < len(freq) && freq[nonzero] == 0 {
		nonzero++
	}

	// query consists only of pruned trigrams -- return all documents
	if nonzero == len(freq) {
		return idx[TAllDocIDs]
	}

	ids := idx.Filter(idx[ts[nonzero]], ts[nonzero+1:])

	return ids
}

// FilterOr removes documents that don't contain any of the list of specified trigrams
// in other words, it's the union of the results of invidial filters
func (idx Index) FilterOr(docs []DocID, tss [][]T) []DocID {

	// no provided filter trigrams
	if len(tss) == 0 {
		return docs
	}
	maxDocs := len(docs)

	filtered := idx.Filter(docs, tss[0])

	for i := 1; i < len(tss); i++ {
		// docs can be a live postings list so we can't repurpose that array
		result := make([]DocID, 0, maxDocs)
		out := idx.Filter(docs, tss[i])
		filtered = union(result, filtered, out)
	}
	return filtered
}

// Filter removes documents that don't contain the specified trigrams
func (idx Index) Filter(docs []DocID, ts []T) []DocID {

	// no provided filter trigrams
	if len(ts) == 0 {
		return docs
	}

	// interesting implementation detail:
	// we don't want to repurpose/alter docs since it's typically
	// a live postings list, hence allocating a result slice
	// however, upon subsequent loop runs we do repurpose the input
	// as the output, because at that point its safe for reuse

	result := make([]DocID, len(docs))

	for _, t := range ts {
		d, ok := idx[t]
		// unknown trigram
		if !ok {
			return nil
		}

		if d == nil {
			// the trigram was removed via Prune()
			continue
		}

		result = intersect(result[:0], docs, d)
		docs = result
	}

	return docs
}

// intersect intersects the input slices and puts the output in result slice
// note that result may be backed by the same array as a or b, since
// we only add docs that also exist in both inputs, it's guaranteed that we
// never overwrite/clobber the input, as long as result's start and len are proper
func intersect(result, a, b []DocID) []DocID {

	var aidx, bidx int

scan:
	for aidx < len(a) && bidx < len(b) {
		if a[aidx] == b[bidx] {
			result = append(result, a[aidx])
			aidx++
			bidx++
			if aidx == len(a) || bidx == len(b) {
				break scan
			}
		}

		for a[aidx] < b[bidx] {
			aidx++
			if aidx == len(a) {
				break scan
			}
		}

		for a[aidx] > b[bidx] {
			bidx++
			if bidx == len(b) {
				break scan
			}
		}
	}

	return result
}

// union takes the union of the input slices
// result slice will be used for output
// specifying a result slice backed by the same array as a or b
// is almost always a bad idea and will clobber your input,
// unless you know what you're doing.
func union(result, a, b []DocID) []DocID {

	var aidx, bidx int

scan:
	for aidx < len(a) && bidx < len(b) {
		if a[aidx] == b[bidx] {
			result = append(result, a[aidx])
			aidx++
			bidx++
			if aidx == len(a) || bidx == len(b) {
				break scan
			}
		}

		for a[aidx] < b[bidx] {
			result = append(result, a[aidx])
			aidx++
			if aidx == len(a) {
				break scan
			}
		}

		for a[aidx] > b[bidx] {
			result = append(result, b[bidx])
			bidx++
			if bidx == len(b) {
				break scan
			}
		}
	}
	// we may have broken out because we either finished b, or a, or both
	// processes any remainders
	for aidx < len(a) {
		result = append(result, a[aidx])
		aidx++
	}

	for bidx < len(b) {
		result = append(result, b[bidx])
		bidx++
	}

	return result
}
