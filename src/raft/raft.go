package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PeerType int

const (
	PeerLeader PeerType = iota
	PeerFollower
	PeerCandidate
)

func (t PeerType) ToString() string {
	switch t {
	case PeerLeader:
		return "Leader"
	case PeerFollower:
		return "Follower"
	case PeerCandidate:
		return "Candidate"
	default:
		return "UNKNOWN"
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh      chan ApplyMsg
	currentState PeerType

	_currentTerm int
	_votedFor    int
	_log         Log
	commitIndex  int
	lastApplied  int

	nextIndex  map[int]int // peerId -> index
	matchIndex map[int]int // peerId -> index
	// can overflow, just need to be different to imply that the leader is alive.
	delta int //  don't overflow within timeout! see ticker()

	condElectionResult *sync.Cond
	heartbeatDuration  time.Duration

	// persistent
	dirty    atomic.Bool // if the persistent fields have been modified
	snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.getCurrentTerm()
	isleader = rf.currentState == PeerLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	if rf.mu.TryLock() {
		panic("Raft::persist: the caller must hole the lock")
	}
	if !rf.dirty.CompareAndSwap(true, false) {
		return
	}
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf._currentTerm)
	encoder.Encode(rf._votedFor)
	encoder.Encode(rf._log)
	raftState := writer.Bytes()
	if len(rf.snapshot) > 0 {
		ServerPrintf(dPersist, rf, "Persist: currentTerm=%v votedFor=%v logs=%v snapshot\n", rf._currentTerm,
			rf._votedFor, rf._log)
		rf.persister.Save(raftState, rf.snapshot)
		rf.snapshot = make([]byte, 0)
	} else {
		ServerPrintf(dPersist, rf, "Persist: currentTerm=%v votedFor=%v logs=%v no snapshot\n", rf._currentTerm,
			rf._votedFor, rf._log)
		rf.persister.Save(raftState, rf.persister.ReadSnapshot())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	ServerPrintf(dPersist, rf, "Server Read Persist\n")

	readBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(readBuffer)
	var currentTerm int
	var votedFor int
	var log Log
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil {
		panic("invalid decode")
	}
	rf._currentTerm = currentTerm
	rf._votedFor = votedFor
	rf._log = log
	rf.dirty.Store(false)

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	ServerPrintf(dSnapshot, rf, "Snapshot index %v\n", index)
	go rf.takeSnapshot(index, snapshot)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	if command == nil {
		panic("Raft::Start: cannot insert nil, you can choose another placeholder\n")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != PeerLeader {
		return rf.commitIndex, rf.getCurrentTerm(), false
	}

	ServerPrintf(dReplicate, rf, "START: %v\n", command)

	rf.AppendLogs(LogEntry{Index: rf.LogLen(), Term: rf.getCurrentTerm(), Cmd: command})

	rf.replicateLogs(nil)

	ServerPrintf(dReplicate, rf, "START RETURN: index=%v, term=%v\n", rf.LastLogIdx(), rf.getCurrentTerm())

	return rf.LastLogIdx(), rf.getCurrentTerm(), true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	prevRequestCnt := 0
	for rf.killed() == false {

		rf.mu.Lock()
		if prevRequestCnt == rf.delta && rf.currentState != PeerLeader {
			ServerPrintf(dTimer, rf, "Election timeout\n")
			// non-blocking election so that the election timeout can kick off
			go func(rf *Raft) { // lock held
				rf.startElection() // lock held on return, released on wait
				if rf.currentState == PeerLeader {
					for peerId := range rf.peers {
						rf.nextIndex[peerId] = max(rf.LogLen(), 1)
						rf.matchIndex[peerId] = 0
					}
					go rf.heartbeat()
				}
				rf.mu.Unlock()
			}(rf)
		} else {
			prevRequestCnt = rf.delta
			rf.mu.Unlock()
		}
		ServerRacePrintf(dTimer, rf, "-> timer")
		time.Sleep(electionTimeout())
		rf.condElectionResult.Broadcast() // force stop elections
		ServerRacePrintf(dTimer, rf, "<- timer")
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	//rf.logApplier = MakeLogApplier(applyCh, rf.me)
	rf.currentState = PeerFollower
	rf._currentTerm = 0
	rf._votedFor = -1
	rf._log = Log{
		InMemIdx: 0,
		Entries:  []LogEntry{{0, 0, ""}},
		TermIdx:  map[int]int{0: 0},
	}
	//rf.commitIndex = 0
	//rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.delta = 0
	rf.mu = sync.Mutex{}
	rf.condElectionResult = sync.NewCond(&rf.mu)
	rf.heartbeatDuration = 100 * time.Millisecond

	rf.snapshot = make([]byte, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastApplied < rf.InMemLogIdx() {
		// handle restarted server
		rf.commitIndex = rf.InMemLogIdx()
		rf.lastApplied = rf.InMemLogIdx() + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
