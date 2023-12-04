package raft

import (
	"6.5840/labrpc"
	"math"
	"math/rand"
	"time"
)

// the caller should hold mu mutex to make the election atomic,
// and it is its responsibility to unlock it
func (rf *Raft) election() {
	if rf.mu.TryLock() {
		panic("the caller should hold mutex before entering the election period")
	}
	rf.currentTerm += 1
	rf.currentState = PeerCandidate
	rf.delta += 1 // reset timer
	DPrintf(dAppd, "S%v(T%v, %v) Reset Timer\n", rf.me, rf.currentTerm, rf.currentState.ToString())
	rf.votedFor = rf.me // vote for itself
	ballet := 1
	DPrintf(dElec, "S%v(T%v, %v) Started election\n", rf.me, rf.currentTerm, rf.currentState.ToString())

	// ask for votes
	for peerId, peer := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(rfRef *Raft, ballet *int, peerId int, peer *labrpc.ClientEnd, localTerm int) {
			//DPrintf(dElec, "S%v(T%v, %v) -> S%v(T%v) Ask Vote\n", rf.me, rf.currentTerm, rf.currentState.ToString(), peerId, localTerm)	// race read

			reply := RequestVoteReply{}
			// cannot use mutating rfRef.* without mutex
			if !peer.Call("Raft.RequestVote", &RequestVoteArgs{
				Term:         localTerm,
				CandidateId:  rfRef.me, // this field is assumed constant
				LastLogIndex: 0,
				LastLogTerm:  0,
			}, &reply) {
				// no reply from the client, retrying?
				return
			}

			rfRef.mu.Lock()
			defer rfRef.mu.Unlock()

			if localTerm != rfRef.currentTerm {
				// maybe this is an obsolete vote reply for the previous election
				return
			}
			// reply.Term >= localTerm = rfRef.currentTerm

			if reply.Term > rfRef.currentTerm {
				DPrintf(dElec, "S%v(T%v, %v) <- S%v(T%v) Discovered New Term\n", rfRef.me, rfRef.currentTerm, rfRef.currentState.ToString(), peerId, reply.Term)
				// discover server with more recent term, it cannot be a candidate because its election term expired
				rfRef.currentState = PeerFollower
				rfRef.currentTerm = reply.Term
				rfRef.votedFor = -1                  // now vote in another term
				rfRef.condElectionResult.Broadcast() // end expired elections (if exists)
				return
			}
			if reply.VoteGranted {
				*ballet += 1
				DPrintf(dElec, "S%v(T%v, %v) <- S%v(T%v) Got Vote. %v/%v\n", rfRef.me, rfRef.currentTerm, rfRef.currentState.ToString(), peerId,
					reply.Term, *ballet, rfRef.majorityCnt())
				// the server may already become a leader by now, so we ignore the vote results
				if rfRef.currentState == PeerCandidate &&
					*ballet >= rfRef.majorityCnt() {
					// majority
					rfRef.currentState = PeerLeader
					rfRef.condElectionResult.Broadcast() // won the election
				}
			}
		}(rf, &ballet, peerId, peer, rf.currentTerm)
	}

	// wait for result
	DPrintf(dElec, "S%v(T%v, %v) Wait Result", rf.me, rf.currentTerm, rf.currentState.ToString())
	rf.condElectionResult.Wait() // releases the mutex, require one when woken up
	DPrintf(dElec, "S%v(T%v, %v) Election Ended state: %v\n", rf.me, rf.currentTerm, rf.currentState.ToString(), rf.currentState.ToString())
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf(dVote, "S%v(T%v, %v) <- S%v(T%v) Request Vote\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.CandidateId, args.Term)
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		DPrintf(dVote, "S%v: T%v > T%v Update\n", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.currentState = PeerFollower // stale term candidate or leader step down
		rf.votedFor = -1               // clear their vote
		rf.condElectionResult.Broadcast()
		// TODO(jens): any initialisations?
	}

	// now currentTerm >= term

	// TODO(jens): persistent
	if rf.currentState == PeerLeader {
		// when a disconnected leader rejoins the cluster, it may have the same term as the candidate
		// since they both re-elect in parallel and can have possibly the same term number
		DPrintf(dVote, "S%v(T%v, %v) leader cannot be asked to vote for S%v(T%v)\n", rf.me, rf.currentTerm, rf.currentState.ToString(),
			args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentState == PeerCandidate && rf.votedFor != rf.me {
		panic("candidate cannot vote for others")
	}

	if args.Term < rf.currentTerm {
		// leader from the previous elections
		DPrintf(dVote, "S%v(T%v, %v) -> S%v(T%v) Reject\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// TODO(jens): check for candidate log
		DPrintf(dVote, "S%v(T%v, %v) -> S%v(T%v) Grant Vote\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.CandidateId, rf.currentTerm)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// GRANTED a vote to a candidate, reset timer
		rf.delta += 1
		DPrintf(dAppd, "S%v(T%v, %v) Reset Timer\n", rf.me, rf.currentTerm, rf.currentState.ToString())
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

	// if a stale leader of more recent term sends a request, we accept it but it won't receive the majority
	// ack and won't commit.
	// case: maybe the server is just online, and this is the first it received, it cannot distinguish
	// the "leaders" since they all have more recent terms.

	if rf.currentState == PeerLeader && rf.currentTerm == args.Term {
		panic("two leaders???")
	}

	if rf.currentTerm <= args.Term {
		// RECEIVED a request from the (more) current leader. can still be stale
		rf.delta += 1 // reset timer by updating it
		DPrintf(dAppd, "S%v(T%v, %v) Reset Timer\n", rf.me, rf.currentTerm, rf.currentState.ToString())
	}

	if rf.currentTerm == args.Term {
		DPrintf(dAppd, "S%v(T%v, %v) Discovered Leader S%v(T%v), Step Down\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.LeaderId, args.Term)
		rf.currentState = PeerFollower
		rf.votedFor = args.LeaderId
	}

	// sync with higher term
	if rf.currentTerm < args.Term {
		// a (more recent, maybe stale) leader has sent a request
		DPrintf(dAppd, "S%v(T%v, %v) Update to T%v\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.Term)
		rf.currentTerm = args.Term
		// follow and vote for that leader as if it won the election
		// and the server is catching up with the majority decision
		rf.currentState = PeerFollower
		rf.votedFor = args.LeaderId
		rf.condElectionResult.Broadcast()
	}

	if args.Term < rf.currentTerm {
		DPrintf(dAppd, "S%v(T%v, %v) -> S%v(T%v) Reject\n", rf.me, rf.currentTerm, rf.currentState.ToString(), args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

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
				DPrintf(dAppd, "S%v(T%v, %v) <- S%v(T%v)\n", rf.me, rf.currentTerm, rf.currentState.ToString(), reply.ServerId, reply.Term)

				if currentTerm < rfRef.currentTerm {
					// we ignore stale response
					return
				}

				if rfRef.currentTerm < reply.Term {
					if reply.Success {
						panic("servers should never accept AppendEntries from stale leader")
					}
					DPrintf(dVote, "S%v: T%v > T%v Update\n", rf.me, reply.Term, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.currentState = PeerFollower // stale leader steps down
					rf.votedFor = -1               // clear their vote
					rf.condElectionResult.Broadcast()
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
