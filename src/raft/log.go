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
	term = log.Entries[log.Len()-1].Term
	index = log.TermIdx[term]
	return
}
