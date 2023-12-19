package raft

type LogEntry struct {
	Index int
	Term  int
	Cmd   interface{}
}

type Log struct {
	InMemIdx int
	Entries  []LogEntry
	TermIdx  map[int]int
}

func (log *Log) At(index int) *LogEntry {
	if index >= log.InMemIdx+len(log.Entries) {
		return nil
	}
	if index < log.InMemIdx {
		return nil
	}
	return &log.Entries[index-log.InMemIdx]
}

func (log *Log) Append(Entries ...LogEntry) int {
	beginIdx := len(log.Entries)
	log.Entries = append(log.Entries, Entries...)

	for _, ent := range Entries {
		if _, ok := log.TermIdx[ent.Term]; !ok {
			log.TermIdx[ent.Term] = ent.Index
			//fmt.Printf("%v\n", log.TermIdx)
		}
	}

	return beginIdx
}

func (log *Log) SubLog(begin, end int) []LogEntry {
	idx0 := begin - log.InMemIdx
	idx1 := end - log.InMemIdx
	if begin == -1 {
		return log.Entries[:idx1]
	}
	if end == -1 {
		return log.Entries[idx0:]
	}
	return log.Entries[idx0:idx1]
}

func (log *Log) AsSubLog(begin, end int) {
	log.Entries = log.SubLog(begin, end)
}

func (log *Log) Len() int {
	return len(log.Entries) + log.InMemIdx
}

func (log *Log) FallBack() (index, term int) {
	term = log.At(log.Len() - 1).Term
	index = log.TermIdx[term]
	return
}

// Snapshot
// discard logs before/until, not including index
func (log *Log) Snapshot(lastIncludedIdx int, lastIncludedTerm int) {
	if lastIncludedIdx < log.InMemIdx {
		panic("Log::Snapshot: already compacted")
	}

	// fast-forward
	if lastIncludedIdx >= log.Len() {
		log.TermIdx = map[int]int{
			lastIncludedTerm: lastIncludedIdx,
		}
		log.Entries = []LogEntry{
			{
				Index: lastIncludedIdx,
				Term:  lastIncludedTerm,
			},
		}
		log.InMemIdx = lastIncludedIdx
		return
	}

	// log prefix: lastIncludedIdx < inMemIdx + len(entries) = len(Log)

	// handle fallback map
	if lastIncludedIdx > log.InMemIdx {
		// idx-1 is in memory
		indexTerm := log.At(lastIncludedIdx - 1).Term
		for term := range log.TermIdx {
			if term < indexTerm {
				delete(log.TermIdx, term)
			} else if term == indexTerm {
				if log.At(lastIncludedIdx).Term == indexTerm {
					// (idx-1, Tx) (idx, Tx) -> move to the next one
					log.TermIdx[term] = lastIncludedIdx
				} else {
					// the next entry is not of the same term: (idx-1, Tx) (idx, Tx+1)
					// delete this term
					delete(log.TermIdx, term)
				}
			} else {
				// we assume the hash function retains the numeric order of the key
				break
			}
		}
	} else {
		// idx is the first log index in memory
	}
	baseIdx := lastIncludedIdx - log.InMemIdx
	log.Entries = log.Entries[baseIdx:]
	log.InMemIdx = lastIncludedIdx
}
