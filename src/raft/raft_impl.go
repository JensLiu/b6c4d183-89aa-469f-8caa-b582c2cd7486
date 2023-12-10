package raft

import (
	"6.5840/labrpc"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Election Begin -------------------------------------------------------------------------------------

// the caller should hold mu mutex to make the election atomic,
// and it is its responsibility to unlock it
func (rf *Raft) startElection() {
	if rf.mu.TryLock() {
		panic("the caller should hold mutex before entering the election period")
	}
	rf.currentTerm += 1
	rf.currentState = PeerCandidate
	rf.resetElectionTimer()
	rf.votedFor = rf.me // vote for itself
	ballot := 1
	ServerPrintf(dElection, rf, "Started election\n")

	// ask for votes
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go rf.collectVote(&ballot, peerId)
	}

	//DPrintf(dElec, "S%v(T%v, %v) Wait Result", rf.me, rf.currentTerm, rf.currentState.ToString())	// race read
	rf.condElectionResult.Wait() // releases the mutex, require one when woken up
	ServerPrintf(dElection, rf, "Election Ended state: %v\n", rf.currentState.ToString())
}

func (rf *Raft) collectVote(ballot *int, peerId int) {
	ServerRacePrintf(dElection, rf, "-> S%v Ask Vote\n", peerId) // race read
	reply := RequestVoteReply{}

	rf.mu.Lock()
	localTerm := rf.currentTerm
	localPeer := rf.peers[peerId]
	lastLogIdx := rf.LastLogIdx()
	lastLogTerm := rf.log.At(lastLogIdx).Term
	rf.mu.Unlock()

	if !localPeer.Call("Raft.RequestVote", &RequestVoteArgs{
		Term:         localTerm,
		CandidateId:  rf.me, // this field is assumed constant
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}, &reply) {
		// no reply from the client, retrying?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if localTerm != rf.currentTerm {
		// Maybe this is an obsolete vote reply for the previous election
		// the timer interrupt will eventually wake up the stale election thread.
		// Do not wake up here since a stale vote can kill the current running election
		return
	}

	// localTerm = rf.currentTerm
	if rf.currentTerm < reply.Term {
		ServerPrintf(dElection, rf, "<- S%v(T%v) Discovered New Term\n", rf.currentState.ToString(), peerId, reply.Term)
		// discover server with more recent term, it cannot be a candidate because its election term expired
		rf.stepDown(reply.Term)
		return
	}

	// rf.currentTerm >= reply.Term
	if reply.VoteGranted {
		*ballot += 1
		ServerPrintf(dElection, rf, "<- S%v(T%v) Got Vote. %v/%v\n", peerId, reply.Term, *ballot, rf.majorityCnt())
		// the server may already become a leader by now, so we ignore the vote results
		if rf.currentState == PeerCandidate && *ballot >= rf.majorityCnt() {
			rf.currentState = PeerLeader
			rf.condElectionResult.Broadcast() // won the election
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ServerPrintf(dElection, rf, "<- S%v(T%v) Request Vote\n", args.CandidateId, args.Term)

	// NOTE: When a disconnected leader rejoins the cluster, it may have the same term as the current leader
	// since they both re-elect in parallel and can have possibly the same term number.
	// But the current leader has already voted for itself and will not vote for it, so no need to handle this case

	if rf.currentTerm < args.Term {
		ServerPrintf(dElection, rf, "Discovers New Term T%v\n", reply.Term)
		rf.stepDown(args.Term)
	}

	rf.applyLog() // apply log if possible

	// currentTerm >= term

	// TODO(jens): persistent

	if rf.currentState == PeerCandidate && rf.votedFor != rf.me {
		panic("candidate cannot vote for others")
	}

	if rf.currentTerm > args.Term {
		// candidate from the previous elections
		ServerPrintf(dElection, rf, "-> S%v(T%v) Reject Vote\n", args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// currentTerm = args.Term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// the second check is for lost reply and the candidate resends the request

		// check candidate log, should be at least as new as the server
		if rf.hasNewerLogThan(args.LastLogIndex, args.LastLogTerm) {
			ServerPrintf(dElection, rf, "-> S%v(%v) Reject, stale log\n", args.CandidateId, args.Term)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		// vote for the candidate
		ServerPrintf(dElection, rf, "-> S%v(T%v) Grant Vote\n", args.CandidateId, rf.currentTerm)
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		// GRANTED a vote to a candidate, election progress, reset timer
		rf.resetElectionTimer()
		return
	}
	ServerPrintf(dElection, rf, "-> S%v(T%v) Ignore\n", args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// Election End -------------------------------------------------------------------------------------

// AppendEntries RPC Begin --------------------------------------------------------------------------
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Only the current leader and the stale leaders can invoke
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ServerId = rf.me // debug
	ServerPrintf(dAppend, rf, "<- S%v(T%v) Args:%v\n", args.LeaderId, args.Term, args)
	ServerPrintf(dAppend, rf, "State: %v\n", rf.ToString())

	if rf.currentState == PeerLeader && rf.currentTerm == args.Term {
		panic("Raft::AppendEntries, two leaders")
	}

	// if a stale leader of more recent term sends a request, we accept it, but it won't receive the majority
	// ack and won't commit.
	// case: maybe the server is just online, and this is the first it received, it cannot distinguish
	// the "leaders" since they all have more recent terms.

	if rf.currentTerm < args.Term {
		// a (more recent, maybe stale) leader has sent a request this rf maybe a stale
		// leader or candidate, and would step down as follower for the new term
		ServerPrintf(dAppend, rf, "Discovers New Term T%v\n", args.Term)
		rf.stepDown(args.Term)
		// Should set voteFor, because there may be other stale leaders running for the same term as the sender
		// after they are reconnected to the cluster.
		// It should not vote for them when called by RequestVote because it follows the current leader
		rf.votedFor = args.LeaderId
	}

	rf.applyLog() // apply if possible

	// currentTerm >= args.Term

	if rf.currentTerm > args.Term {
		// reject stale leader for log sync
		ServerPrintf(dAppend, rf, "-> S%v(T%v) Reject Log Sync\n", args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// currentTerm = args.Term

	rf.currentState = PeerFollower // Candidate may step down for the elected leader
	rf.votedFor = args.LeaderId
	rf.resetElectionTimer() // progress

	if !rf.logConsistencyCheck(args) {
		ServerPrintf(dAppend, rf, "Failed Integrity Check\n")
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	ServerPrintf(dAppend, rf, "-> S%v(T%v) Accept\n", args.LeaderId, args.Term)
	ServerPrintf(dAppend, rf, "State: %v\n", rf.ToString())
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) logConsistencyCheck(args *AppendEntriesArgs) bool {
	lastLogIdx := rf.LastLogIdx()
	if args.PrevLogIndex > lastLogIdx {
		// the current log does not contain the index
		ServerPrintf(dAppend, rf, "args.PrevLogIndex > lastLogIdx\n")
		return false
	}

	// prevLogIdx <= lastLogIdx
	if rf.log.At(args.PrevLogIndex).Term != args.PrevLogTerm {
		ServerPrintf(dAppend, rf, "rf.log[args.PrevLogIndex].Term != args.PrevLogTerm\n")
		return false
	}

	// now the prefix matches
	diffIdx := -1
	baseIdx := args.PrevLogIndex + 1
	for i, ent := range args.Entries {
		idx := baseIdx + i
		if idx > rf.LastLogIdx() || rf.log.At(idx).Term != ent.Term {
			// remove entries after the conflicting term
			rf.log.AsSubLog(-1, idx)
			diffIdx = idx
			break
		}
	}

	// it may be the case that no entries conflict with the request, but
	// entries after entries the leader sends are stale

	// apply entries not already in the log
	if diffIdx != -1 {
		ServerPrintf(dAppend, rf, "diffIdx = %v\n", diffIdx)
		rf.log.Append(args.Entries[diffIdx-baseIdx:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		ServerPrintf(dAppend, rf, "args.LeaderCommit > rf.commitIndex")
		// if indexOf(LastLogTheLeaderSends) < leaderCommit, we may end up committing stale entries??
		rf.commitIndex = min(args.LeaderCommit, rf.LastLogIdx())
	}

	return true
}

// AppendEntries RPC End --------------------------------------------------------------------------

// Log Replication Begin --------------------------------------------------------------------------

func (rf *Raft) replicateLogs(condWait *sync.Cond) {
	if rf.mu.TryLock() {
		panic("Raft::replicateLogs: the caller should hold the lock")
	}

	idx := rf.LastLogIdx()

	ServerRacePrintf(dReplicate, rf, "Start Replicate Logs until %v\n", idx)

	for peerId, peer := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go rf.askToReplicate(peerId, peer, condWait)
	}
}

func (rf *Raft) askToReplicate(peerId int, peer *labrpc.ClientEnd, condWait *sync.Cond) {

	reply := AppendEntriesReply{}

	rf.mu.Lock()
	localTerm := rf.currentTerm
	beginLogIdx := rf.nextIndex[peerId] // nextIdx[.] >= 1, log 0 is always committed
	prevLogIdx := beginLogIdx - 1
	//prevLogIdx := rf.matchIndex[peerId]
	prevLogTerm := rf.log.At(prevLogIdx).Term
	entries := rf.log.SubLog(prevLogIdx+1, -1)
	leaderCommit := rf.commitIndex
	args := AppendEntriesArgs{
		Term:         localTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	ServerPrintf(dReplicate, rf, "Args: %v", args.ToString())
	ServerPrintf(dReplicate, rf, "->S%v Ask To Replicate", peerId)
	rf.mu.Unlock()

	peer.Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	ServerPrintf(dReplicate, rf, "<-S%v Reply: %v", reply.ServerId, reply)

	// reply for previous term, ignore
	if rf.currentTerm > localTerm {
		return
	}

	// discovers new term
	if reply.Term > rf.currentTerm {
		ServerPrintf(dReplicate, rf, "Discovered new term %v\n", reply.Term)
		rf.stepDown(reply.Term)
		return
	}

	if reply.Term < rf.currentTerm {
		// the RPC API returns if it fails. distinguish this from a slow follower
		if reply.Term != 0 {
			panic("Raft::askToReplicate: reply term < current term\n")
		}
		ServerPrintf(dReplicate, rf, "Failed reply from T%v, Ignore\n", reply.Term)
		return
	}

	// replyTerm = currentTerm

	if reply.Success {
		peerUpdatedLastIdx := prevLogIdx + len(entries)
		rf.nextIndex[peerId] = max(rf.nextIndex[peerId], peerUpdatedLastIdx+1) // deal with stale reply
		rf.matchIndex[peerId] = max(rf.matchIndex[peerId], peerUpdatedLastIdx)

		if rf.currentTerm != reply.Term {
			return
		}

		// currentTerm = replyTerm
		// only count majority for the current term to avoid reverted commit from previous terms if the leader
		// crashes and the entry does not have a majority over {server...}\{leader} cluster

		ackCnt := 1
		for id := range rf.peers {
			if id == rf.me {
				continue
			}
			if rf.matchIndex[id] >= peerUpdatedLastIdx {
				ackCnt += 1
			}
		}

		ServerPrintf(dReplicate, rf, "<- S%v(T%v) Replicated %v/%v\n", reply.ServerId, reply.Term,
			ackCnt, rf.majorityCnt())

		if ackCnt >= rf.majorityCnt() { // the majority stored the log
			if rf.commitIndex < peerUpdatedLastIdx {
				rf.commitIndex = peerUpdatedLastIdx
				ServerPrintf(dCommit, rf, "Committed\n")
				rf.applyLog()
			} else {
				ServerPrintf(dReplicate, rf, "Ignore Stale Reply\n")
			}
			ServerPrintf(dCommit, rf, "state: %v\n", rf.ToString())
			if condWait != nil {
				condWait.Broadcast()
			}
		}
		return
	}
	rf.handleStaleFollower(peerId, &reply)

}

func (rf *Raft) handleStaleFollower(peerId int, reply *AppendEntriesReply) {
	if rf.mu.TryLock() {
		panic("Raft::handleStaleFollower: the caller should hold the lock")
	}
	nextIdx := rf.nextIndex[peerId] - 1
	ServerPrintf(dReplicate, rf, "State %v\n", rf.ToString())
	rf.nextIndex[peerId] = max(1, nextIdx)
	ServerPrintf(dReplicate, rf, "Slow Follower %v\n", peerId)
}

func (rf *Raft) applyLog() {
	if rf.mu.TryLock() {
		panic("Raft::applyLog: the caller should hold the lock")
	}
	// applied index is always at most equal to the commit index
	// thus do not check for overflow
	for rf.commitIndex >= rf.lastApplied {
		// use `for` (not `if`)
		// because the leader may ignore a stale commit message from its follower
		// (if reply being reordered), so it may lose the chance to apply
		// we apply as much as possible when called
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.log.At(rf.lastApplied).Cmd,
			CommandIndex:  rf.lastApplied,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		ServerPrintf(dApply, rf, "Apply command %v, %v\n", rf.lastApplied, rf.log.At(rf.lastApplied))
		rf.lastApplied += 1
	}
}

// Log Replication End --------------------------------------------------------------------------

// Helpers ----------------------------------------------------------------------------------------
func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	ServerPrintf(dTimer, rf, "Heartbeat Start\n")

	for rf.currentState == PeerLeader {
		rf.replicateLogs(nil)
		rf.mu.Unlock()
		time.Sleep(rf.heartbeatDuration)
		rf.mu.Lock()
	}

	ServerPrintf(dTimer, rf, "Heartbeat Stop\n")
	rf.mu.Unlock()
}

func (rf *Raft) majorityCnt() int {
	return int(math.Ceil(float64(len(rf.peers)) / 2))
}

func electionTimeout() time.Duration {
	return time.Duration(120+(rand.Int63()%300)) * time.Millisecond
}

func (rf *Raft) stepDown(term int) {
	if rf.mu.TryLock() {
		panic("Raft::stepDown: the caller must hold the lock before entering")
	}
	// step down as the follower of the more recent term
	rf.currentTerm = term
	rf.currentState = PeerFollower
	rf.votedFor = -1
	rf.condElectionResult.Broadcast() // end stale elections
	rf.resetElectionTimer()           // it finds a more recent term, progress
}

func (rf *Raft) hasNewerLogThan(lastLogIdx1, lastLogTerm1 int) bool {
	if rf.mu.TryLock() {
		panic("Raft::hasNewerLog: caller should hold the lock")
	}
	lastLogIdx0 := rf.LastLogIdx()
	lastLogTerm0 := rf.log.At(lastLogIdx0).Term
	ServerPrintf(dElection, rf,
		"currentLastLogIdx=%v, currentLastLogTerm=%v, lastLogIdx=%v, lastLogTerm=%v\n",
		lastLogIdx0, lastLogTerm0, lastLogIdx1, lastLogTerm1)
	if lastLogTerm0 != lastLogTerm1 {
		return lastLogTerm0 > lastLogTerm1
	}
	return lastLogIdx0 > lastLogIdx1
}

func (rf *Raft) resetElectionTimer() {
	if rf.mu.TryLock() {
		panic("Raft::resetElectionTimer: the caller must hold the lock before entering")
	}
	rf.delta += 1
	ServerPrintf(dTimer, rf, "Reset Timer\n")
}

func (rf *Raft) ToString() string {
	return fmt.Sprintf("currentTerm=%v, votedFor=%v, committedIndex=%v, "+
		"lastApplied=%v, nextIndex=%v, matchIndex=%v", rf.currentTerm, rf.votedFor,
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	//return ""
}

func (rf *Raft) LastLogIdx() int {
	return rf.log.Len() - 1
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's ID so that the follower can redirect the client to the leader
	PrevLogIndex int        // index of the log immediately preceding new ones
	PrevLogTerm  int        // therm of the previous log
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commit index. The follower should commit until this point
}

func (args *AppendEntriesArgs) ToString() string {
	return fmt.Sprintf("Term=%v, Leader=%v, PrevLogIndex=%v, PrevLogTerm=%v, LeaderCommit=%v",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit,
	)
}

type AppendEntriesReply struct {
	Term     int  // for stale leaders to update themselves
	Success  bool // if the follower passes the validation check
	ServerId int  // for debug reasons
}
