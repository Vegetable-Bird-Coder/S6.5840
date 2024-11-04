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
	//	"bytes"

	"fmt"
	"math/rand"
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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

const HEARTBEAT int = 150

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Additional state
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg // Channel to send committed entries to service
	applyCond      *sync.Cond    // CV to wake up applier goroutine after committing new entries
	replicatorCond []*sync.Cond  // CVs to signal replicator goroutines to batch replicating entries
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply represents the reply for AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // Conflict term
	XIndex  int // First index of the conflict term
	XLen    int // Number of logs
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		fmt.Printf("Raft %d term %d refuses to vote for Raft %d term %d\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	if !rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		fmt.Printf("Raft %d term %d refuses to vote for Raft %d term %d\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	}

	rf.votedFor = args.CandidateID
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.electionTimer.Reset(randomElectionTimeout())

	fmt.Printf("Raft %d term %d votes for Raft %d term %d\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
}

func (rf *Raft) logConflict(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	lastLogEntry := rf.getLastLogEntry()
	if args.PrevLogIndex > lastLogEntry.Index {
		reply.XTerm, reply.XLen, reply.Success = -1, lastLogEntry.Index+1, false
		fmt.Printf("Raft %d term %d refuses entries from Raft %d term %d: shorter log\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		return true
	}

	// Check if log contains an entry at PrevLogIndex with term matching PrevLogTerm
	firstLogEntryIndex := rf.getFirstLogEntry().Index
	if rf.log[args.PrevLogIndex-firstLogEntryIndex].Term != args.PrevLogTerm {
		reply.XTerm, reply.Success = rf.log[args.PrevLogIndex-firstLogEntryIndex].Term, false
		index := args.PrevLogIndex - 1
		for index >= firstLogEntryIndex && rf.log[index-firstLogEntryIndex].Term == reply.XTerm {
			index--
		}
		reply.XIndex = index + 1
		fmt.Printf("Raft %d term %d refuses entries from Raft %d term %d: conflict term\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		return true
	}

	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		fmt.Printf("Raft %d term %d refuses entries from Raft %d term %d: smaller term\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.state = FOLLOWER
	rf.electionTimer.Reset(randomElectionTimeout())

	if rf.logConflict(args, reply) {
		return
	}

	defer rf.persist()

	// Append any new entries not already in the log
	firstLogEntryIndex := rf.getFirstLogEntry().Index
	rf.log = rf.log[0 : args.PrevLogIndex-firstLogEntryIndex+1]
	rf.log = append(rf.log, args.Entries...)

	// Update commitIndex if leaderCommit is greater
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogEntry().Index)
		rf.applyCommittedEntries()
	}

	reply.Term, reply.Success = rf.currentTerm, true
	fmt.Printf("Raft %d term %d accepts entries from Raft %d term %d\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
}

// sendApplyMsg sends committed entries to the applyCh.
func (rf *Raft) applyCommittedEntries() {
	firstLogEntryIndex := rf.getFirstLogEntry().Index
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-firstLogEntryIndex].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
		fmt.Printf("Raft %d applied log index %d\n", rf.me, rf.lastApplied)
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}

	// Append the new command to the leader's log
	lastLogEntry := rf.appendLogEntry(command)
	rf.broadcastAppendEntries(false)
	return lastLogEntry.Index, lastLogEntry.Term, true
}

// func (rf *Raft) sendAppendEntriesWithRetry(server int, args *AppendEntriesArgs) {
// 	backoff := time.Duration(100 * time.Millisecond)
// 	maxRetries := 5

// 	for retries := 0; !rf.killed() && retries < maxRetries; retries++ {
// 		rf.mu.Lock()
// 		if rf.state != Leader {
// 			rf.mu.Unlock()
// 			return
// 		}

// 		var reply AppendEntriesReply
// 		ok := rf.sendAppendEntries(server, args, &reply)
// 		if !ok || reply.Term > rf.currentTerm {
// 			if reply.Term > rf.currentTerm {
// 				rf.currentTerm = reply.Term
// 				rf.state = Follower
// 				rf.votedFor = -1
// 				rf.resetElectionTimeout()
// 			}
// 			rf.mu.Unlock()
// 			return
// 		}

// 		if reply.Success {
// 			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
// 			rf.nextIndex[server] = rf.matchIndex[server] + 1
// 			rf.updateCommitIndex()
// 			rf.mu.Unlock()
// 			return
// 		} else {
// 			if reply.XTerm != -1 {
// 				for i := len(rf.log) - 1; i >= 0; i-- {
// 					if rf.log[i].Term == reply.XTerm {
// 						rf.nextIndex[server] = i + 1
// 						break
// 					} else if rf.log[i].Term < reply.XTerm {
// 						rf.nextIndex[server] = reply.XIndex
// 						break
// 					}
// 				}
// 			} else {
// 				rf.nextIndex[server] = reply.XLen
// 			}
// 			args.PrevLogIndex = rf.nextIndex[server] - 1
// 			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
// 			args.Entries = rf.log[rf.nextIndex[server]:]
// 			rf.mu.Unlock()
// 		}

// 		time.Sleep(backoff)
// 		backoff *= 2
// 	}
// }

func (rf *Raft) updateCommitIndex() {
	n := rf.commitIndex + 1
	for n <= len(rf.log)-1 {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.log[n].Term == rf.currentTerm {
			rf.commitIndex = n
			fmt.Printf("Raft %d updates commitIndex to %d\n", rf.me, rf.commitIndex)
			rf.applyCommittedEntries()
			n++
		} else {
			break
		}
	}
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
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.startElection()
			}
			rf.electionTimer.Reset(randomElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.broadcastAppendEntries(true)
				rf.heartbeatTimer.Reset(time.Duration(HEARTBEAT) * time.Millisecond)
			}
			rf.mu.Unlock()
		}
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]LogEntry, 1),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          FOLLOWER,
		electionTimer:  time.NewTimer(randomElectionTimeout()),
		heartbeatTimer: time.NewTimer(time.Duration(HEARTBEAT)),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	lastLogEntry := rf.getLastLogEntry()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLogEntry.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// go rf.applier()

	return rf
}

func (rf *Raft) startElection() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	fmt.Printf("Raft %d term %d start election\n", rf.me, rf.currentTerm)

	request := rf.makeRequestVoteRequest()

	votes := 1
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				response := new(RequestVoteReply)
				if rf.sendRequestVote(peer, request, response) {
					rf.mu.Lock()
					if rf.currentTerm == request.Term && rf.state == CANDIDATE {
						if response.VoteGranted {
							votes++
							if votes > len(rf.peers)/2 {
								rf.state = LEADER
								rf.broadcastAppendEntries(true)
								rf.heartbeatTimer.Reset(time.Duration(HEARTBEAT) * time.Millisecond)
							}
						} else if response.Term > rf.currentTerm {
							rf.state = FOLLOWER
							rf.electionTimer.Reset(randomElectionTimeout())
							rf.currentTerm, rf.votedFor = response.Term, -1
							rf.persist()
						}
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	request := rf.makeAppendEntriesRequest(peer)
	rf.mu.Unlock()

	response := new(AppendEntriesReply)
	if rf.sendAppendEntries(peer, request, response) {
		rf.mu.Lock()
		rf.handleAppendEntriesResponse(peer, request, response)
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {

}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	if isHeartbeat {
		fmt.Printf("Raft %d term %d starts sending heartbeats\n", rf.me, rf.currentTerm)
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.replicateOneRound(peer)
			}
		}
	} else {
		fmt.Printf("Raft %d term %d starts replicating entry\n", rf.me, rf.currentTerm)
		for peer := range rf.peers {
			if peer != rf.me {
				rf.replicatorCond[peer].Signal()
			}
		}
	}
}

// Check if the candidate log is up-to-date.
func (rf *Raft) isUpToDate(lastLogIndex, lastLogTerm int) bool {
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	return lastTerm < lastLogTerm || (lastTerm == lastLogTerm && lastIndex <= lastLogIndex)
}

func (rf *Raft) getFirstLogEntry() *LogEntry {
	return &rf.log[0]
}

// used under mu.Lock()
func (rf *Raft) getLastLogEntry() *LogEntry {
	return &rf.log[len(rf.log)-1]
}

// used under mu.Lock()
func (rf *Raft) makeRequestVoteRequest() *RequestVoteArgs {
	lastLogEntry := rf.getLastLogEntry()
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
}

func (rf *Raft) makeAppendEntriesRequest(peer int) *AppendEntriesArgs {
	nextLogIndex := rf.nextIndex[peer]
	firstLogEntry := rf.getFirstLogEntry()
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: nextLogIndex - 1,
		PrevLogTerm:  rf.log[nextLogIndex-1].Term,
		Entries:      rf.log[nextLogIndex-firstLogEntry.Index:],
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) appendLogEntry(command interface{}) *LogEntry {
	lastLogEntry := rf.getLastLogEntry()
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm, Index: lastLogEntry.Index + 1})
	rf.persist()
	return rf.getLastLogEntry()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

