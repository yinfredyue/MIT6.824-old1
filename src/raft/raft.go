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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	follower  int = 0
	candidate int = 1
	leader    int = 2

	sleepUnit int = 50

	electionTimeoutMin int = 500
	electionTimeoutMax int = 600
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State not mentioned in Figure 2
	applyCh     chan ApplyMsg // For sending ApplyMsg back to the client
	serverState int           // follower: 0, candidate: 1, leader: 2

	// For candidate -> leader
	numServers    int // number of servers in the cluster
	minMajority   int // min number of servers in a majority
	votesReceived int // For candidate only, number of received votes

	// For follower/candidate election timeout
	electionTimeout time.Duration // In miliseconds
	prevCommTime    time.Time     // time when receiving previous rpc call.

	// For leader heartbeat
	heartbeatInterval     time.Duration // In miliseconds, >= 100
	prevAppendEntriesTime time.Time     // time when generating previous heartbeat

	// For leader sending ApplyMsg back to client
	applyCond *sync.Cond

	// States mentioned in Figure 2
	//
	// Persistent stae on all servers
	// Updated on stable storage before responding to RPCs
	currentTerm int        // Latest term server has seen, init to 0
	votedFor    int        // candidateID that received vote in current term. Use int for this field, -1 to represent nil
	log         []LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (init to 0)
	lastApplied int // index of highest log entry applied to state machine (init to 0)

	// Violate state on leaders
	// Reinitialized after election
	nextIndex  []int // Index of the next log entry to send to each server, init to leader last log index + 1
	matchIndex []int // Index of highest log entry known to be replicated on each server (init to 0)
}

//
// A Go object representing a log entry.
//
type LogEntry struct {
	Command      interface{} // command for state machine
	ReceivedTerm int         // term when entry was received by the leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.serverState == leader

	// Your code here (2A).
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// resetElectionTimeout must be called when holding the lock
//
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(electionTimeoutMin+rand.Intn(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond
	rf.prevCommTime = time.Now()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidateID of the candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogterm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	rf.becomesFollowerIfOutOfTerm(args.Term)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("%v receives RequestVote from %v: not granted as candidate out of term\n", rf.me, args.CandidateID)
	} else {
		// If a server is a candidate/leader, then it would not grant vote
		// to any RequestVote. On becoming a candidate, the server votes for
		// itself. A leader is a candidate.

		// If votedFor is null or candidatedID
		votedForCond := rf.votedFor == -1 || rf.votedFor == args.CandidateID

		// And candidate's log is at least up-to-date as receiver's log, grant vote
		lastLogTerm := -1 // Should be -1, instead of 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].ReceivedTerm
		}
		lastLogIndex := len(rf.log) - 1
		upToDateCond1 := args.LastLogterm > lastLogTerm
		upToDateCond2 := args.LastLogterm == lastLogTerm && args.LastLogIndex >= lastLogIndex

		if votedForCond && (upToDateCond1 || upToDateCond2) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID

			DPrintf("%v receives RequestVote from %v: granted\n", rf.me, args.CandidateID)

			// Update election timeout only if granting vote
			// rf.prevCommTime = time.Now()
			rf.resetElectionTimeout()
		} else { // Without this else branch, zero-value is used in reply
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			DPrintf("%v receives RequestVote from %v: not granted as follower already voted, or candidate's log less up-to-date\n", rf.me, args.CandidateID)
		}
		DPrintf("args: {Term: %v, CandidateID: %v, LsatLogIndex: %v, LastLogTerm: %v}\n", args.Term, args.CandidateID, args.LastLogIndex, args.LastLogterm)
	}

	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should pass &reply.
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// server: index of the target server in rf.peers[]
	// args: POINTER to RPC arguments
	// reply: POINTER to RequestVoteReply to be filled in
	// Call() returns true if a reply is received, otherwise false.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// RequestVote RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // For follower's to redirect clients
	PrevLogIndex int        // index of the log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat, multiple for efficiency)
	LeaderCommit int        // leader's commitIndex
}

//
// RequestVote RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	rf.becomesFollowerIfOutOfTerm(args.Term)

	if args.Term < rf.currentTerm {
		// DPrintf("%v receives AppendEntries from %v. args.prevLogIndex = %v. Reply FALSE as candidate is out of date\n", rf.me, args.LeaderID, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if args.PrevLogIndex >= 0 && // For heartbeat, args.PrevLogIndex = -1
		(len(rf.log) < args.PrevLogIndex+1 ||
			rf.log[args.PrevLogIndex].ReceivedTerm != args.PrevLogTerm) {
		// Log doesn't contain an entry at prevLogIndex with term prevLogTerm
		// DPrintf("%v receives AppendEntries from %v. args.prevLogIndex = %v. Reply FALSE as \n", rf.me, args.LeaderID, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
	} else { // This branch should accept heartbeat, if not out-of-date
		// Received AppendEntries from new leader
		// Convert to follower (if needed)
		if rf.serverState != follower {
			DPrintf("%v becomes follower  because of receiving AppendEntries from current leader %v", rf.me, args.LeaderID)
			rf.serverState = follower
		}
		rf.votedFor = -1
		rf.votesReceived = 0

		// Not mentioned in Figure 2
		// Update election timeout only if receiving AppendEntries from current leader
		// rf.prevCommTime = time.Now()
		rf.resetElectionTimeout()

		// If an existing entry conflicts with a new one (same index but
		// different terms), delete the existing entry and all that follow it.
		deleteStart := len(rf.log)
		for i := args.PrevLogIndex + 1; i < len(rf.log); i++ {
			indexInEnties := i - (args.PrevLogIndex + 1)
			if args.Entries[indexInEnties].ReceivedTerm != rf.log[i].ReceivedTerm {
				// Delete the existing entry and all following entries
				deleteStart = i
				break
			}
		}
		rf.log = rf.log[:deleteStart] // deleteStart <= len(rf.log)

		// Append any new entries not already in the log
		for i := deleteStart - (args.PrevLogIndex + 1); i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
			rf.commitIndex = min(args.LeaderCommit, indexOfLastNewEntry)

			// Possible change to rf.commitIndex
			// Increment lastApplied, and apply log[lasApplied] to state machine
			// Use rf.applyCond to signal applyCommandRoutine()
			rf.applyCond.Broadcast()
		}

		reply.Term = rf.currentTerm
		reply.Success = true
	}

	rf.mu.Unlock()
}

//
// Go routine for applying command to the state machine. This is the only code
// that can increment rf.lastApplied.
//
func (rf *Raft) applyCommandRoutine() {
	for {
		rf.mu.Lock()

		rf.applyCond.Wait() // Wait for signal of rf.commitIndex being advanced

		if rf.commitIndex > rf.lastApplied {
			// Increment lastApplied
			rf.lastApplied++

			// Apply log[lastApplied] to state machine
			// Nothing really need to be done except for leader
			if rf.serverState == leader {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: len(rf.log) - 1,
				}

				rf.mu.Unlock() // Release lock before blocking

				// Sending through channel is blocking, so the order is preserved.
				rf.applyCh <- applyMsg

				rf.mu.Lock()
			}
		} else {
			time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
		}

		rf.mu.Unlock()
	}
}

//
// Send AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// pc, _, _, okk := runtime.Caller(1)
	// details := runtime.FuncForPC(pc)
	// if okk && details != nil {
	// 	fmt.Printf("called from %s\n", details.Name())
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.commitIndex + 1
	term := rf.currentTerm
	isLeader := rf.serverState == leader
	if isLeader {
		// Append entry to entry log
		logEntry := LogEntry{Command: command, ReceivedTerm: rf.currentTerm}
		rf.log = append(rf.log, logEntry)

		// Respond after entry applied to state machine done, done in applyCommandRoutine
	}
	return index, term, isLeader
}

//
// electionTimeoutRoutine would be a separate background goroutine.
//
func (rf *Raft) electionTimeoutRoutine() {
	for {
		rf.mu.Lock()

		if rf.serverState == leader || // is a leader
			time.Now().Before(rf.prevCommTime.Add(rf.electionTimeout)) { // not timeout yet
			// - A leader, or
			// - A follower/candidate, but not timeout yet
			rf.mu.Unlock() // Release the lock before sleep
			time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
		} else {
			// Follower/Candidate, timeout reached
			// If election timeout elapses without receiving AppendEntries RPC from
			// current leader or granting vote to candidate: convert to candidate
			// On conversoin to candidate, start election (4 steps).
			rf.serverState = candidate // 0. Become candidate
			DPrintf("%v Becomes candidate at election timeout. rf.currentTerm: %v -> %v \n", rf.me, rf.currentTerm, rf.currentTerm+1)
			rf.currentTerm++     // 1. increment current term
			rf.votesReceived = 1 // 2. Vote for self
			rf.votedFor = rf.me
			// rf.prevCommTime = time.Now() // 3. Reset election timer
			rf.resetElectionTimeout()

			/// Check voteCountImplementation.txt for more detail ///
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				lastLogIndex := len(rf.log) - 1
				lastLogTerm := -1
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].ReceivedTerm
				}
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogterm:  lastLogTerm,
				}

				go func(server int, args RequestVoteArgs) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(server, &args, &reply) // could block

					rf.mu.Lock()
					if ok {
						if rf.currentTerm != args.Term { // Term confusion in the student's guide
							return
						}
						// Only if the two terms are the same, contine processing
						rf.becomesFollowerIfOutOfTerm(reply.Term)
						if rf.serverState == candidate && reply.VoteGranted {
							rf.votesReceived++
							if rf.votesReceived >= rf.minMajority {
								rf.serverState = leader
								DPrintf("%v Becomes leader! ", rf.me)

								// Initialize leader-only server state
								rf.nextIndex = make([]int, len(rf.peers))
								for i := range rf.nextIndex {
									rf.nextIndex[i] = len(rf.log) // Initialized to leader last log index + 1
								}
								rf.matchIndex = make([]int, len(rf.peers))
								for i := range rf.matchIndex {
									rf.matchIndex[i] = 0 // Initialized to 0
								}
							}
						}
					}
					rf.mu.Unlock()
				}(i, args)
			}
			/// Check voteCountImplementation.txt for more detail ///

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) logReplicationRoutine() {
	for {
		rf.mu.Lock()

		if rf.serverState != leader {
			rf.mu.Unlock() // Release lock before sleep
			time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
			continue
		}

		// Is a leader
		for i := range rf.nextIndex {
			lastLogIndex := len(rf.log) - 1

			if lastLogIndex < rf.nextIndex[i] {
				continue
			}

			// If last log index >= nextIndex for a follower
			// Send AppendEntries RPC with log entries starting at nextIndex
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].ReceivedTerm
			}
			entries := rf.log[rf.nextIndex[i]:]
			go rf.sendAppendEntriesUntilSucceedOrOutOfDate(i, rf.currentTerm,
				rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex)

			rf.prevAppendEntriesTime = time.Now()
		}

		// If there exists an N such that N > commitIndex, a majority of
		// matchIndex[i] >= N and log[N].term == currentTerm:
		// set commitindex = N
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			count := 0
			for j := range rf.matchIndex {
				if rf.matchIndex[j] >= N {
					count++
				}
			}
			if count >= rf.minMajority && rf.log[N].ReceivedTerm == rf.currentTerm {
				rf.commitIndex = N
				rf.applyCond.Signal() // Apply command
				break
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesUntilSucceedOrOutOfDate(server int, currentTerm int, me int,
	prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) {

	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}

	// Keeps trying until succeed
	for {
		ok := rf.sendAppendEntries(server, &args, &reply) // blocking

		if ok {
			if rf.currentTerm != currentTerm {
				// From student's guide:
				// Compare currentTerm with sent term
				// If different, drop and return without processing old reply
				return // Should return instead of continue
			}

			rf.mu.Lock()
			rf.becomesFollowerIfOutOfTerm(reply.Term)
			rf.mu.Unlock()

			if reply.Success {
				// Update nextIndex and matchIndex for followers
				rf.mu.Lock()

				lastLogIndex := prevLogIndex + len(entries)
				rf.nextIndex[server] = lastLogIndex + 1
				rf.matchIndex[server] = lastLogIndex

				rf.mu.Unlock()
				return
			} else if reply.Term == rf.currentTerm {
				// If AppendEntries fails due to log inconsistency,
				// decrement nextIndex and retry
				rf.mu.Lock()
				rf.nextIndex[server]--
				if rf.nextIndex[server] == -1 {
					return
				}

				// Retry
				DPrintf("%v retries AppendEntries to %v, args.PrevLogIndex: %v -> %v\n", rf.me, server, args.PrevLogIndex, args.PrevLogIndex-1)
				args.PrevLogIndex = args.PrevLogIndex - 1
				DPrintf("Updated args.PrevLogIndex: %v\n", args.PrevLogIndex)
				args.PrevLogTerm = rf.log[args.PrevLogIndex].ReceivedTerm
				args.Entries = rf.log[rf.nextIndex[server]:]
				rf.mu.Unlock()
			} else {
				// AppendEntries fail due to leader out of date, do not retry
				// Step down to be a follower.
				// Already handled by rf.becomesFollowerIfOutOfTerm() above
				return // Should return here, otherwise infinite loop!
			}
		}
	}
}

//
// periodicHeartbeatRoutine would be a separate background goroutine.
//
func (rf *Raft) periodicHeartbeatRoutine() {
	for {
		rf.mu.Lock()

		notLeader := rf.serverState != leader
		tooSoon := time.Now().Before(rf.prevAppendEntriesTime.Add(rf.heartbeatInterval))

		if notLeader || tooSoon {
			rf.mu.Unlock() // Release the lock before sleep
			time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
		} else {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				// Have to use argument passing. Refer to Rule 5 of
				// https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.log[prevLogIndex].ReceivedTerm
				}

				go func(server int, currentTerm int, me int, commitIndex int, prevLogIndex int, prevLogTerm int) {
					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderID:     me,
						Entries:      make([]LogEntry, 0),
						LeaderCommit: commitIndex,

						// PrevLogIndex and PrevLogTerm are set in the same way as normal AppendEntries
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
					}
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)

					if ok {
						rf.mu.Lock()
						rf.becomesFollowerIfOutOfTerm(reply.Term)
						rf.mu.Unlock()
					}
				}(i, rf.currentTerm, rf.me, rf.commitIndex, prevLogIndex, prevLogTerm)
			}
			rf.prevAppendEntriesTime = time.Now()

			rf.mu.Unlock()
		}
	}
}

//
// The caller of becomesFollowerIfOutOfTerm must hold the lock
// throughout the calling process.
//
func (rf *Raft) becomesFollowerIfOutOfTerm(replyOrResponseTerm int) {
	// If response contains term T > currentTerm:
	// Set currentTerm to T and convert to follower.
	if replyOrResponseTerm > rf.currentTerm {
		DPrintf("%v Becomes follower as being out of term!  rf.currentTerm: %v -> %v", rf.me, rf.currentTerm, replyOrResponseTerm)
		rf.currentTerm = replyOrResponseTerm
		rf.serverState = follower

		// Not mentioned in Figure 2
		rf.votedFor = -1
		rf.votesReceived = 0
		// rf.prevCommTime = time.Now()
		rf.resetElectionTimeout()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.serverState = follower // A server starts up as a follower
	DPrintf("%v Starts out as follower! ", rf.me)

	rf.numServers = len(rf.peers)
	rf.minMajority = (rf.numServers / 2) + 1
	rf.votesReceived = 0

	rand.Seed(time.Now().UnixNano())
	rf.resetElectionTimeout()
	rf.prevCommTime = time.Now().Add(-1 * time.Second) // Early enough

	// Add * time.Millsecond is important! Otherwise the count of RPC calls explodes.
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.prevAppendEntriesTime = time.Now().Add(-1 * time.Second) // Early enough

	rf.applyCond = sync.NewCond(&rf.mu)

	// Initialization for persistent states on all servers (might not be necessary)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	// initalize volatile states on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// No initialization for leader-only state until becoming a leader

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Create a background go routine to start leader election periodically by
	// sending out RequestVote RPCs if it hasn't heard from others for a while.
	go rf.electionTimeoutRoutine()

	// Create a background routine for periodic heartbeat
	go rf.periodicHeartbeatRoutine()

	// Create a background routine for applying command to state machine if possible
	go rf.applyCommandRoutine()

	// Create a background routine for log replication where leader send AppendEntries
	go rf.logReplicationRoutine()

	return rf
}
