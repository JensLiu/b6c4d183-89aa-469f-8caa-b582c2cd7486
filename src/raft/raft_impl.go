package raft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"slices"
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

	defer rf.persist()

	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.currentState = PeerCandidate
	rf.resetElectionTimer()
	rf.setVotedFor(rf.me) // vote for itself
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
	localTerm := rf.getCurrentTerm()
	localPeer := rf.peers[peerId]
	lastLogIdx := rf.LastLogIdx()
	lastLogTerm := rf.LogAt(lastLogIdx).Term
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

	if localTerm != rf.getCurrentTerm() {
		// Maybe this is an obsolete vote reply for the previous election
		// the timer interrupt will eventually wake up the stale election thread.
		// Do not wake up here since a stale vote can kill the current running election
		return
	}

	// localTerm = rf.currentTerm
	if rf.getCurrentTerm() < reply.Term {
		ServerPrintf(dElection, rf, "<- S%v(T%v) Discovered New Term\n", rf.currentState.ToString(), peerId, reply.Term)
		// discover server with more recent term, it cannot be a candidate because its election term expired
		rf.stepDown(reply.Term)
		rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ServerPrintf(dElection, rf, "<- S%v(T%v) Request Vote\n", args.CandidateId, args.Term)

	// NOTE: When a disconnected leader rejoins the cluster, it may have the same term as the current leader
	// since they both re-elect in parallel and can have possibly the same term number.
	// But the current leader has already voted for itself and will not vote for it, so no need to handle this case

	if rf.getCurrentTerm() < args.Term {
		ServerPrintf(dElection, rf, "Discovers New Term T%v\n", reply.Term)
		rf.stepDown(args.Term)
	}

	//rf.applyLogs() // apply log if possible

	// currentTerm >= term

	if rf.currentState == PeerCandidate && rf.getVotedFor() != rf.me {
		panic("candidate cannot vote for others")
	}

	if rf.getCurrentTerm() > args.Term {
		// candidate from the previous elections
		ServerPrintf(dElection, rf, "-> S%v(T%v) Reject Vote\n", args.CandidateId, args.Term)
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false
		return
	}

	// currentTerm = args.Term
	if rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateId {
		// the second check is for lost reply and the candidate resends the request

		// check candidate log, should be at least as new as the server
		if rf.hasNewerLogThan(args.LastLogIndex, args.LastLogTerm) {
			ServerPrintf(dElection, rf, "-> S%v(%v) Reject, stale log\n", args.CandidateId, args.Term)
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = false
			return
		}

		// vote for the candidate
		ServerPrintf(dElection, rf, "-> S%v(T%v) Grant Vote\n", args.CandidateId, rf.getCurrentTerm())
		rf.setVotedFor(args.CandidateId)
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = true

		// GRANTED a vote to a candidate, election progress, reset timer
		rf.resetElectionTimer()
		return
	}
	ServerPrintf(dElection, rf, "-> S%v(T%v) Ignore\n", args.CandidateId, args.Term)
	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = false
}

// Election End -------------------------------------------------------------------------------------

// AppendEntries RPC Begin --------------------------------------------------------------------------
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Only the current leader and the stale leaders can invoke
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.persist()

	reply.ServerId = rf.me // debug
	ServerPrintf(dAppend, rf, "<- S%v(T%v) Args:%v\n", args.LeaderId, args.Term, args)
	ServerPrintf(dAppend, rf, "State: %v\n", rf.ToString())

	if rf.currentState == PeerLeader && rf.getCurrentTerm() == args.Term {
		panic("Raft::AppendEntries, two leaders")
	}

	// if a stale leader of more recent term sends a request, we accept it, but it won't receive the majority
	// ack and won't commit.
	// case: maybe the server is just online, and this is the first it received, it cannot distinguish
	// the "leaders" since they all have more recent terms.

	if rf.getCurrentTerm() < args.Term {
		// a (more recent, maybe stale) leader has sent a request this rf maybe a stale
		// leader or candidate, and would step down as follower for the new term
		ServerPrintf(dAppend, rf, "Discovers New Term T%v\n", args.Term)
		rf.stepDown(args.Term)
		// Should set voteFor, because there may be other stale leaders running for the same term as the sender
		// after they are reconnected to the cluster.
		// It should not vote for them when called by RequestVote because it follows the current leader
		rf.setVotedFor(args.LeaderId)
	}

	rf.applyLogs() // apply if possible

	// currentTerm >= args.Term

	if rf.getCurrentTerm() > args.Term {
		// reject stale leader for log sync
		ServerPrintf(dAppend, rf, "-> S%v(T%v) Reject Log Sync\n", args.LeaderId, args.Term)
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
		return
	}

	// currentTerm = args.Term

	rf.currentState = PeerFollower // Candidate may step down for the elected leader
	// the isolated leader will never have term higher than the acting leader, since it does not start a new election
	rf.setVotedFor(args.LeaderId)
	rf.resetElectionTimer() // progress

	if !rf.logConsistencyCheck(args) {
		ServerPrintf(dAppend, rf, "Failed Integrity Check\n")
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
		reply.FallbackIndex, reply.FallbackTerm = rf._log.FallBack()
		return
	}

	ServerPrintf(dAppend, rf, "-> S%v(T%v) Accept\n", args.LeaderId, args.Term)
	ServerPrintf(dAppend, rf, "State: %v\n", rf.ToString())
	reply.Term = rf.getCurrentTerm()
	reply.Success = true
}

func (rf *Raft) logConsistencyCheck(args *AppendEntriesArgs) bool {
	lastLogIdx := rf.LastLogIdx()
	if args.PrevLogIndex > lastLogIdx {
		// the current log does not contain the index
		ServerPrintf(dAppend, rf, "args.PrevLogIndex(%v) > lastLogIdx(%v)\n", args.PrevLogIndex, lastLogIdx)
		return false
	}

	// prevLogIdx <= lastLogIdx
	if args.PrevLogIndex < rf.InMemLogIdx() {
		ServerPrintf(dAppend, rf, "asking for compacted logs %v while the in memory log is %v\n",
			args.PrevLogIndex, rf.InMemLogIdx())
		return false
	}

	if rf.LogAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		ServerPrintf(dAppend, rf, "rf.log[args.PrevLogIndex].Term != args.PrevLogTerm\n")
		return false
	}

	// now the prefix matches
	diffIdx := -1
	baseIdx := args.PrevLogIndex + 1
	for i, ent := range args.Entries {
		idx := baseIdx + i
		if idx > rf.LastLogIdx() || rf.LogAt(idx).Term != ent.Term {
			// remove entries after the conflicting term
			rf.AsSubLog(-1, idx)
			diffIdx = idx
			break
		}
	}

	// it may be the case that no entries conflict with the request, but
	// entries after entries the leader sends are stale

	// apply entries not already in the log
	if diffIdx != -1 {
		ServerPrintf(dAppend, rf, "diffIdx = %v\n", diffIdx)
		rf.AppendLogs(args.Entries[diffIdx-baseIdx:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		ServerPrintf(dAppend, rf, "args.LeaderCommit > rf.commitIndex")

		if args.PrevLogIndex+len(args.Entries) < args.LeaderCommit {
			// if indexOf(LastLogTheLeaderSends) < leaderCommit, we may end up committing stale entries??
			panic("may commit stale entry?")
		}

		rf.commitIndex = min(args.LeaderCommit, rf.LastLogIdx())
	}

	return true
}

// AppendEntries RPC End --------------------------------------------------------------------------

// InstallSnapshot RPC Begin ----------------------------------------------------------------------

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.getCurrentTerm()

	ServerPrintf(dSnapshot, rf, "<- S%v(T%v) InstallSnapshot lastIdx=%v, lastTerm=%v\n", args.LeaderId,
		args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.Term > rf.getCurrentTerm() {
		rf.stepDown(args.Term)
	}

	// term <= current Term

	if args.Term < rf.getCurrentTerm() {
		return
	}

	// term = currentTerm

	if args.LastIncludedIndex < rf.InMemLogIdx() {
		return
	}

	if args.LastIncludedIndex == rf.InMemLogIdx() &&
		rf.LogAt(args.LastIncludedIndex).Term != args.LastIncludedTerm {
		// overwrite the stale log, since snapshot entries are committed, thus permanent
		rf.LogAt(args.LastIncludedIndex).Term = args.LastIncludedTerm
	}

	ServerPrintf(dSnapshot, rf, "Before snapshot state: %v\n", rf.ToString())

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.snapshot = args.Data // rf.persist will write it back to the disk
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex + 1

	rf.LogSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)

	ServerPrintf(dSnapshot, rf, "After snapshot state: %v\n", rf.ToString())
}

// InstallSnapshot RPC End ------------------------------------------------------------------------

// Log Replication Begin --------------------------------------------------------------------------

func (rf *Raft) replicateLogs(condWait *sync.Cond) {
	if rf.mu.TryLock() {
		panic("Raft::replicateLogs: the caller should hold the lock")
	}

	defer rf.persist()

	ServerRacePrintf(dReplicate, rf, "Start Replicate Logs\n")

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
	if rf.currentState != PeerLeader {
		// avoid change of state between the function call and RPC call
		rf.mu.Unlock()
		return
	}

	localTerm := rf.getCurrentTerm()
	beginLogIdx := max(rf.nextIndex[peerId], 1) // nextIdx[.] >= 1, log 0 is always committed
	prevLogIdx := beginLogIdx - 1

	if prevLogIdx < rf.InMemLogIdx() {
		rf.useSnapshotToSyncFollower(peerId)
		rf.mu.Unlock()
		return
	}

	prevLogTerm := rf.LogAt(prevLogIdx).Term
	entries := rf.SubLog(prevLogIdx+1, -1)
	leaderCommit := rf.commitIndex
	args := AppendEntriesArgs{
		Term:         localTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	ServerPrintf(dReplicate, rf, "->S%v Ask To Replicate, Args %v\n", peerId, args.ToString())
	rf.mu.Unlock()

	peer.Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != PeerLeader {
		// avoid change of state during the call, it may discover a new leader
		return
	}
	ServerPrintf(dReplicate, rf, "<-S%v Reply: %v, PrevLogIndex %v, EntryLen %v\n", reply.ServerId, reply, prevLogIdx, len(entries))
	// reply for previous term, ignore
	if rf.getCurrentTerm() > localTerm {
		return
	}

	// discovers new term
	if reply.Term > rf.getCurrentTerm() {
		ServerPrintf(dReplicate, rf, "Discovered new term %v\n", reply.Term)
		rf.stepDown(reply.Term)
		return
	}

	if reply.Term < rf.getCurrentTerm() {
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

		if rf.getCurrentTerm() != reply.Term {
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
				rf.applyLogs()
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

	if rf.nextIndex[peerId] == beginLogIdx {
		rf.handleStaleFollower(peerId, &reply)
		return
	} else {
		// otherwise, the index has been changed by another request; we should not overwrite it
		ServerPrintf(dAppend, rf, "stale reply for fallback\n")
	}

}

func (rf *Raft) handleStaleFollowerNotOptimised(peerId int, reply *AppendEntriesReply) {
	if rf.mu.TryLock() {
		panic("Raft::handleStaleFollower: the caller should hold the lock")
	}
	nextIdx := rf.nextIndex[peerId] - 1
	ServerPrintf(dReplicate, rf, "State %v\n", rf.ToString())
	rf.nextIndex[peerId] = max(1, nextIdx)
	ServerPrintf(dReplicate, rf, "Slow Follower %v\n", peerId)
}

func (rf *Raft) handleStaleFollower(peerId int, reply *AppendEntriesReply) {
	if rf.mu.TryLock() {
		panic("Raft::handleStaleFollower: the caller should hold the lock")
	}

	if reply.FallbackIndex < rf.InMemLogIdx() ||
		reply.FallbackIndex == rf.InMemLogIdx() && rf.LogAt(rf.InMemLogIdx()).Cmd == nil {
		ServerPrintf(dSnapshot, rf, "S%v wants to fallback to log %v that's compacted\n",
			reply.ServerId, reply.FallbackIndex)
		rf.nextIndex[peerId] = reply.FallbackIndex // the next heartbeat will use snapshot
		return
	}

	from := rf.nextIndex[peerId]

	// bypass log dirty detection, since we do not modify log entries
	_, ok := rf._log.TermIdx[reply.FallbackTerm]

	var searchTerm int
	if ok {
		if rf.LogAt(reply.FallbackIndex).Term == reply.FallbackTerm {
			// the prefix matches, we should fast-forward to this point rather than -1 everytime
			rf.nextIndex[peerId] = reply.FallbackIndex
			ServerPrintf(dAppend, rf, "nextIndex[%v]: %v -> %v\n", peerId, from, rf.nextIndex[peerId])
			return
		} else {
			// not the same term, fallback one more
			searchTerm = reply.FallbackTerm - 1
		}
	} else { // !ok
		searchTerm = reply.FallbackTerm
	}

	// binary search to find a matching term <= searchTerm in the leader
	keys := make([]int, len(rf._log.TermIdx))
	for key := range rf._log.TermIdx {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	i, found := slices.BinarySearch(keys, searchTerm)
	if !found {
		i = max(1, i-1)
	}
	rf.nextIndex[peerId] = rf._log.TermIdx[keys[i]]

	to := rf.nextIndex[peerId]
	ServerPrintf(dAppend, rf, "nextIndex[%v]: %v -> %v\n", peerId, from, to)
}

func (rf *Raft) useSnapshotToSyncFollower(peerId int) {
	if rf.mu.TryLock() {
		panic("Raft::useSnapshotToSyncFollower: the caller should hold the lock")
	}

	defer rf.persist()

	localTerm := rf.getCurrentTerm()
	lastIncludedIndex := rf.InMemLogIdx()
	lastIncludedTerm := rf.LogAt(lastIncludedIndex).Term
	data := rf.persister.ReadSnapshot()

	// validation
	i := SnapshotLastIncludedIndex(data)
	if i != lastIncludedIndex {
		panic("inconsistent snapshot")
	}
	// validation

	if len(data) == 0 {
		panic("no snapshot")
	}
	args := InstallSnapshotArgs{
		Term:              localTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	reply := InstallSnapshotReply{}

	ServerPrintf(dSnapshot, rf, "-> S%v InstallSnapshot lastIdx=%v, lastTerm=%v\n", peerId, args.LastIncludedIndex, args.LastIncludedTerm)
	ServerPrintf(dSnapshot, rf, "Before InstallSnapshot Request State: %v\n", rf.ToString())

	rf.mu.Unlock()
	rf.peers[peerId].Call("Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()

	ServerPrintf(dSnapshot, rf, "<- S%v InstallSnapshot Reply: %v\n", peerId, reply)

	if rf.getCurrentTerm() != localTerm {
		return
	}

	if reply.Term == 0 {
		// failed/timeout term
		return
	}

	if rf.getCurrentTerm() < reply.Term {
		rf.stepDown(reply.Term)
	}

	// since the follower is behind
	rf.matchIndex[peerId] = lastIncludedIndex
	rf.nextIndex[peerId] = lastIncludedIndex + 1

	ServerPrintf(dSnapshot, rf, "After InstallSnapshot Request State: %v\n", rf.ToString())
}

func (rf *Raft) applyLogs() int {
	if rf.mu.TryLock() {
		panic("Raft::applyLog: the caller should hold the lock")
	}

	// applied index is always at most equal to the commit index
	// thus do not check for overflow
	applied := 0

	for rf.commitIndex >= rf.lastApplied {
		// use `for` (not `if`)
		// because the leader may ignore a stale commit message from its follower
		// (if reply being reordered), so it may lose the chance to apply
		// we apply as much as possible when called
		cmd := rf.LogAt(rf.lastApplied).Cmd
		if cmd == nil {
			// placeholder for lastIncluded in snapshot
			panic("last included")
		}
		msg := ApplyMsg{
			CommandValid:  rf.lastApplied != 0,
			Command:       cmd,
			CommandIndex:  rf.lastApplied,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		ServerPrintf(dAppend, rf, "State: %v\n", rf.ToString())
		ServerPrintf(dApply, rf, "Start Apply command %v, %v\n", rf.lastApplied, rf.LogAt(rf.lastApplied))
		rf.applyCh <- msg
		ServerPrintf(dApply, rf, "End Apply command %v, %v\n", rf.lastApplied, rf.LogAt(rf.lastApplied))
		rf.lastApplied += 1
		applied += 1
	}
	return applied
}

// Log Replication End --------------------------------------------------------------------------

// Snapshot Begin -------------------------------------------------------------------------------

// takeSnapshot is called asynchronously, otherwise the may result in deadlock
// because snapshot and command share the same apply-channel
func (rf *Raft) takeSnapshot(index int, snapshot []byte) {

	// NOTE(jens): it may be the case:
	//	Leader -> Sx: Replicate Log[a,b] --------------------+
	//													     + -----> Sx <- Leader: Receive, replicate
	//	Leader <- Snapshot, Compact to Log[b:]			     + <----- Sx -> Leader: Accept replicate Log[a,b]
	// 	Leader -> Sx Heartbeat, Replicate Log[a,b] ----> ... |
	//	...													 |
	// 	Leader <- Sx: Accept replicate Log[a,b]	<------------+

	// this may result in the leader sending Sx to replicate entries that's been compacted:
	// the leader hasn't received the ack from Sx, thus resending the entries that have been compacted at heartbeat,
	// which may result in sending the unnecessary snapshots multiple times.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	ServerPrintf(dSnapshot, rf, "Snapshot index=%v\n", index)

	if index < rf.InMemLogIdx() {
		return
	}

	// since the API passed the whole snapshot as a param, we assume that it can fit in memory
	defer rf.persist()
	rf.snapshot = snapshot

	// validation of snapshot
	if index != SnapshotLastIncludedIndex(snapshot) {
		panic("Raft::takeSnapshot: snapshot index mismatch")
	}

	// check commit
	if rf.commitIndex < index {
		panic("Raft::takeSnapshot: snapshotting uncommitted index\n")
	}
	ServerPrintf(dSnapshot, rf, "before state: %v\n", rf.ToString())
	inMenIdx := rf.InMemLogIdx()
	if index < inMenIdx {
		ServerPrintf(dSnapshot, rf, "stale snapshot request: ask %v but in fact >= %v\n", index, rf.InMemLogIdx())
		return
	}
	rf.LogSnapshot(index, rf.LogAt(index).Term)
	ServerPrintf(dSnapshot, rf, "after state: %v\n", rf.ToString())
}

// Snapshot end ---------------------------------------------------------------------------------

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
	rf.setCurrentTerm(term)
	rf.currentState = PeerFollower
	rf.setVotedFor(-1)
	rf.condElectionResult.Broadcast() // end stale elections
	rf.resetElectionTimer()           // it finds a more recent term, progress
}

func (rf *Raft) hasNewerLogThan(lastLogIdx1, lastLogTerm1 int) bool {
	if rf.mu.TryLock() {
		panic("Raft::hasNewerLog: caller should hold the lock")
	}
	lastLogIdx0 := rf.LastLogIdx()
	lastLogTerm0 := rf.LogAt(lastLogIdx0).Term
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
		"lastApplied=%v, nextIndex=%v, matchIndex=%v, log=%v", rf.getCurrentTerm(), rf.getVotedFor(),
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf._log)
	//return ""
}

// persistent fields accessor ------------------------------------

func (rf *Raft) LogAt(index int) *LogEntry {
	if index < rf._log.InMemIdx {
		panic("Raft::LogAt: ask for compacted log\n")
	}
	if index >= rf._log.InMemIdx+len(rf._log.Entries) {
		panic("Raft::LogAt: ask for invalid log\n")
	}
	return rf._log.At(index)
}

func (rf *Raft) AppendLogs(entries ...LogEntry) int {
	rf.dirty.Store(true)
	ServerPrintf(dPersist, rf, "Appended logs, Dirty\n")
	return rf._log.Append(entries...)
}

func (rf *Raft) SubLog(begin, end int) []LogEntry {
	return rf._log.SubLog(begin, end)
}

func (rf *Raft) AsSubLog(begin, end int) {
	rf.dirty.Store(true)
	ServerPrintf(dPersist, rf, "Trimed as sublog, Dirty\n")
	rf._log.AsSubLog(begin, end)
}

func (rf *Raft) LogLen() int {
	return rf._log.Len()
}

func (rf *Raft) LastLogIdx() int {
	return rf._log.Len() - 1
}

func (rf *Raft) InMemLogIdx() int { return rf._log.InMemIdx }

// LogSnapshot trims/fast-forwards the log to fit the snapshot
func (rf *Raft) LogSnapshot(lastIncludedIdx, lastIncludedTerm int) {
	rf.dirty.Store(true)
	ServerPrintf(dPersist, rf, "Discard logs, Dirty\n")
	rf._log.Snapshot(lastIncludedIdx, lastIncludedTerm)
}

func (rf *Raft) getCurrentTerm() int {
	return rf._currentTerm
}

func (rf *Raft) getVotedFor() int {
	return rf._votedFor
}

func (rf *Raft) setCurrentTerm(term int) {
	if term == rf._currentTerm {
		return // avoid writing to disk
	}
	rf.dirty.Store(true)
	ServerPrintf(dPersist, rf, "Set currentTerm, Dirty\n")
	rf._currentTerm = term
}

func (rf *Raft) setVotedFor(votedFor int) {
	if votedFor == rf._votedFor {
		return
	}
	rf.dirty.Store(true)
	ServerPrintf(dPersist, rf, "Set votedFor, Dirty\n")
	rf._votedFor = votedFor
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
	return fmt.Sprintf("Term=%v, Leader=%v, PrevLogIndex=%v, PrevLogTerm=%v, LeaderCommit=%v, entries=%v",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries,
	)
}

type AppendEntriesReply struct {
	Term          int  // for stale leaders to update themselves
	Success       bool // if the follower passes the validation check
	ServerId      int  // for debug reasons
	FallbackIndex int  // for fast follower log correction
	FallbackTerm  int  // for fast follower log correction
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// we do not split snapshots
	//Offset            int
	//Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func SnapshotLastIncludedIndex(snapshot []byte) int {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var snapLastIncludedIndex int
	if d.Decode(&snapLastIncludedIndex) != nil {
		panic("snapshot Decode() error")
	}
	return snapLastIncludedIndex
}
