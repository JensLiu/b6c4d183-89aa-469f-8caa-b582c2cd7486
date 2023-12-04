package raft

import (
	"6.5840/labrpc"
	"math"
	"math/rand"
	"time"
)

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
	ballet := 1
	DPrintf(dElec, "S%v(T%v, %v) Started election\n", rf.me, rf.currentTerm, rf.currentState.ToString())

	// ask for votes
	for peerId, peer := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go rf.collectVote(&ballet, peerId, peer, rf.currentTerm)
	}

	//DPrintf(dElec, "S%v(T%v, %v) Wait Result", rf.me, rf.currentTerm, rf.currentState.ToString())	// race read
	rf.condElectionResult.Wait() // releases the mutex, require one when woken up
	DPrintf(dElec, "S%v(T%v, %v) Election Ended state: %v\n", rf.me, rf.currentTerm, rf.currentState.ToString(), rf.currentState.ToString())
}

func (rf *Raft) collectVote(ballet *int, peerId int, localPeer *labrpc.ClientEnd, localTerm int) {
	//DPrintf(dElec, "S%v(T%v, %v) -> S%v(T%v) Ask Vote\n", rf.me, rf.currentTerm, rf.currentState.ToString(), peerId, localTerm)	// race read
	reply := RequestVoteReply{}
	// cannot use mutating rfRef.* without mutex
	if !localPeer.Call("Raft.RequestVote", &RequestVoteArgs{
		Term:         localTerm,
		CandidateId:  rf.me, // this field is assumed constant
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, &reply) {
		// no reply from the client, retrying?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if localTerm != rf.currentTerm {
		// maybe this is an obsolete vote reply for the previous election
		return
	}
	// localTerm = rf.currentTerm

	if rf.currentTerm < reply.Term {
		DPrintf(dElec, "S%v(T%v, %v) <- S%v(T%v) Discovered New Term\n", rf.me, rf.currentTerm, rf.currentState.ToString(), peerId, reply.Term)
		// discover server with more recent term, it cannot be a candidate because its election term expired
		rf.stepDown(reply.Term)
		return
	}

	// rf.currentTerm >= reply.Term
	if reply.VoteGranted {
		*ballet += 1
		DPrintf(dElec, "S%v(T%v, %v) <- S%v(T%v) Got Vote. %v/%v\n", rf.me, rf.currentTerm, rf.currentState.ToString(), peerId,
			reply.Term, *ballet, rf.majorityCnt())
		// the server may already become a leader by now, so we ignore the vote results
		if rf.currentState == PeerCandidate && *ballet >= rf.majorityCnt() {
			rf.currentState = PeerLeader
			rf.condElectionResult.Broadcast() // won the election
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf(dVote, "S%v(T%v, %v) <- S%v(T%v) Request Vote\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.CandidateId, args.Term)
	defer rf.mu.Unlock()

	// NOTE: When a disconnected leader rejoins the cluster, it may have the same term as the current leader
	// since they both re-elect in parallel and can have possibly the same term number.
	// But the current leader has already voted for itself and will not vote for it, so no need to handle this case

	if rf.currentTerm < args.Term {
		DPrintf(dVote, "S%v(T%v) Discovers New Term T%v\n", rf.me, args.Term, rf.currentTerm)
		rf.stepDown(args.Term)
	}

	// currentTerm >= term

	// TODO(jens): persistent

	if rf.currentState == PeerCandidate && rf.votedFor != rf.me {
		panic("candidate cannot vote for others")
	}

	if rf.currentTerm > args.Term {
		// candidate from the previous elections
		DPrintf(dVote, "S%v(T%v, %v) -> S%v(T%v) Reject Vote\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// currentTerm = args.Term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// the second check is for lost reply and the candidate resend the request
		// TODO(jens): check for candidate log
		DPrintf(dVote, "S%v(T%v, %v) -> S%v(T%v) Grant Vote\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.CandidateId, rf.currentTerm)
		// vote for the candidate
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		// GRANTED a vote to a candidate, election progress, reset timer
		rf.resetElectionTimer()
		return
	}
	DPrintf(dVote, "S%v(T%v, %v) -> S%v(T%v) Ignore\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Only the current leader and the stale leaders can invoke
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ServerId = rf.me // debug
	DPrintf(dAppd, "S%v(T%v, %v) <- S%v(T%v)\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.LeaderId, args.Term)

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
		DPrintf(dAppd, "S%v(T%v, %v) Discovers New Term T%v\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.Term)
		rf.stepDown(args.Term)
		// Should set voteFor, because there may be other stale leaders running for the same term as the sender
		// after they are reconnected to the cluster.
		// It should not vote for them when called by RequestVote because it follows the current leader
		rf.votedFor = args.LeaderId
	}

	// currentTerm >= args.Term

	if rf.currentTerm > args.Term {
		// reject stale leader for log sync
		DPrintf(dAppd, "S%v(T%v, %v) -> S%v(T%v) Reject Log Sync\n",
			rf.me, rf.currentTerm, rf.currentState.ToString(), args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// currentTerm = args.Term

	rf.currentState = PeerFollower // Candidate may step down for the elected leader
	rf.votedFor = args.LeaderId
	rf.resetElectionTimer() // progress

	if len(args.Entires) == 0 {
		DPrintf(dAppd, "S%v(T%v, %v) -> S%v(T%v) Confirm Heartbeat\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		// NOTE: should return false for heartbeat messages
		reply.Success = false
		return
	}
	// TODO(jens): follower sync log
	// TODO(jens): follower reset ticker
	DPrintf(dAppd, "S%v(T%v, %v) -> S%v(T%v) Accept\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	DPrintf(dTimr, "S%v(T%v, %v) Heartbeat Start\n", rf.me, rf.currentTerm, rf.currentState.ToString())

	for rf.currentState == PeerLeader {
		for peerId, peer := range rf.peers {
			if peerId == rf.me {
				continue
			}
			go func(rfRef *Raft, peer *labrpc.ClientEnd, peerId int, currentTerm int) {
				reply := AppendEntriesReply{}
				//DPrintf(dAppd, "S%v(T%v, %v) -> S%v Heartbeat\n", rf.me, rf.currentTerm, rf.currentState.ToString(), peerId) // race read
				peer.Call("Raft.AppendEntries", &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rfRef.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entires:      make([]interface{}, 0),
					LeaderCommit: 0,
				}, &reply)

				rfRef.mu.Lock()
				defer rfRef.mu.Unlock()
				DPrintf(dAppd, "S%v(T%v, %v) <- S%v(T%v) AppendEntries Reply\n", rf.me, rf.currentTerm, rf.currentState.ToString(), reply.ServerId, reply.Term)

				if currentTerm < rfRef.currentTerm {
					// we ignore stale response
					return
				}

				if rfRef.currentTerm < reply.Term {
					if reply.Success {
						panic("servers should never accept AppendEntries from stale leader")
					}
					DPrintf(dAppd, "S%v(T%v) Discovers New Term T%v\n", rf.me, reply.Term, rf.currentTerm)
					rf.stepDown(reply.Term)
				}

			}(rf, peer, peerId, rf.currentTerm)
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartbeatDuration)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) majorityCnt() int {
	return int(math.Ceil(float64(len(rf.peers)) / 2))
}

func electionTimeout() time.Duration {
	return time.Duration(50+(rand.Int63()%300)) * time.Millisecond
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

func (rf *Raft) resetElectionTimer() {
	if rf.mu.TryLock() {
		panic("Raft::resetElectionTimer: the caller must hold the lock before entering")
	}
	rf.delta += 1
	DPrintf(dTimr, "S%v(T%v, %v) Reset Timer\n", rf.me, rf.currentTerm, rf.currentState.ToString())
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
	Term         int           // leader's term
	LeaderId     int           // leader's ID so that the follower can redirect the client to the leader
	PrevLogIndex int           // index of the log immediately preceding new ones
	PrevLogTerm  int           // therm of the previous log
	Entires      []interface{} // log entries to store (empty for heartbeat)
	LeaderCommit int           // leader's commit index. The follower should commit until this point
}

type AppendEntriesReply struct {
	ServerId int  // for debug reasons
	Term     int  // for stale leaders to update themselves
	Success  bool // if the follower passes the validation check
}
