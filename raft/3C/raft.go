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

	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Leader    = iota // Leader constant
	Follower         // Follower constant
	Candidate        // Candidate constant
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
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cv        *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int
	applyCh   chan ApplyMsg

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

	heartbeatTimeout   time.Duration
	requestVoteChan    chan bool
	appendEntriesChan  chan bool
	resetElectionTimer chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// under rf.mu.Lock()
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
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	raftState := buffer.Bytes()
	rf.persister.Save(raftState, nil)
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

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm, votedFor int
	var logs []LogEntry
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&logs) != nil {
		fmt.Errorf("read persist error\n")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		fmt.Printf("Raft %d term %d with %d logs back\n", rf.me, rf.currentTerm, len(rf.log))
		rf.printLogInfo()
	}
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
	CandidateId  int
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
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term of confilicting entry
	XIndex  int // index of first entry whose term is Xterm
	XLen    int // length of empty logs
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm

	if args.Term < rf.currentTerm {
		fmt.Printf("Raft %d term %d refuses to vote for Raft %d term %d\n", rf.me, currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	persistState := false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // new term starts
		rf.state = Follower
		persistState = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.resetElectionTimer <- true
		fmt.Printf("Raft %d term %d votes for Raft %d term %d\n", rf.me, currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		persistState = true
	} else {
		fmt.Printf("Raft %d term %d refuses to vote for Raft %d term %d\n", rf.me, currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	if persistState {
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetElectionTimer <- true

	currentTerm := rf.currentTerm
	// fmt.Printf("Raft %d term %d receive AppendEntries message from Raft %d term %d\n", rf.me, currentTerm, args.LeaderId, args.Term)

	if args.Term < rf.currentTerm {
		fmt.Printf("Raft %d term %d refuses AppendEntries message from Raft %d term %d because lower term\n", rf.me, currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	persistState := false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		persistState = true
	}

	reply.Term = rf.currentTerm

	if len(rf.log)-1 < args.PrevLogIndex { // log is too short
		if args.Entries == nil {
			fmt.Printf("Raft %d term %d refuses Heartbeat message from Raft %d term %d prevIndex %d prevTerm %d because log is too short\n", rf.me, currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		} else {
			fmt.Printf("Raft %d term %d refuses AppendEntries message from Raft %d term %d prevIndex %d prevTerm %d because log is too short\n", rf.me, currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		}
		rf.printLogInfo()
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - len(rf.log) + 1
		reply.Success = false
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // Term confliction
		if args.Entries == nil {
			fmt.Printf("Raft %d term %d refuses Heartbeat message from Raft %d term %d prevIndex %d prevTerm %d because term confliction\n", rf.me, currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		} else {
			fmt.Printf("Raft %d term %d refuses AppendEntries message from Raft %d term %d prevIndex %d prevTerm %d because term confliction\n", rf.me, currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		}
		rf.printLogInfo()
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = rf.findTermFirstIndex(reply.XTerm)
		reply.Success = false
	} else {
		if args.Entries == nil {
			fmt.Printf("Raft %d term %d accept Heartbeat message from Raft %d term %d\n", rf.me, currentTerm, args.LeaderId, args.Term)
			rf.log = rf.log[:args.PrevLogIndex+1] // remove redundant logs
		} else {
			fmt.Printf("Raft %d term %d accept AppendEntries message from Raft %d term %d\n", rf.me, currentTerm, args.LeaderId, args.Term)
			rf.log = rf.log[:args.PrevLogIndex+1] // remove redundant logs
			rf.log = append(rf.log, args.Entries...)
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.log)-1 { // set min val
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.applyLogEntries()
		}
		reply.Success = true
		persistState = true
	}

	if persistState {
		rf.persist()
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntires(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendAppendEntiresDealWithReply(server, nextIndex int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// fmt.Printf("Raft %d term %d send to Raft %d with nextIndex %d\n", rf.me, rf.currentTerm, server, nextIndex)

	prevLogIndex := nextIndex - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      rf.log[nextIndex:len(rf.log)],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}

	if rf.sendAppendEntires(server, &args, &reply) {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.resetElectionTimer <- true
			rf.persist()
			return
		}

		if reply.Success {
			if args.PrevLogIndex+len(args.Entries) < rf.matchIndex[server] {
				return
			}

			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.cv.Broadcast()

			for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
				count := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}
				if count >= (len(rf.peers)+1)/2 && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
					rf.applyLogEntries()
					break
				}
			}
		} else { // fast backup
			fmt.Printf("Raft %d term %d start backup to Raft %d\n", rf.me, rf.currentTerm, server)
			rf.printLogInfo()
			if reply.XTerm != -1 {
				termLastIndex := rf.findTermLastIndex(reply.XTerm)
				if termLastIndex == -1 {
					nextIndex = reply.XIndex
				} else {
					nextIndex = termLastIndex + 1
				}
			} else {
				nextIndex -= reply.XLen
			}
			go rf.sendAppendEntiresDealWithReply(server, nextIndex)
		}
	} else { // retry
		if rf.killed() {
			return
		}
		
		go rf.sendAppendEntiresDealWithReply(server, nextIndex)
	}
}

func (rf *Raft) startReplication(index int) {
	fmt.Printf("Raft %d term %d start replication at index %d\n", rf.me, rf.currentTerm, len(rf.log)-1)

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				for rf.nextIndex[server] < index {
					rf.cv.Wait()
				}

				if !rf.killed() && rf.nextIndex[server] == index {
					go rf.sendAppendEntiresDealWithReply(server, index)
				}
			}(i)
		}
	}

}

// under mu.Lock()
func (rf *Raft) findTermFirstIndex(term int) int {
	for idx := len(rf.log) - 1; idx > 0; idx-- {
		if rf.log[idx].Term == term && rf.log[idx-1].Term != term {
			return idx
		}
	}
	return 0
}

// under mu.Lock()
func (rf *Raft) findTermLastIndex(term int) int {
	for idx := len(rf.log) - 1; idx >= 0; idx-- {
		if rf.log[idx].Term == term {
			return idx
		}
	}
	return -1
}

// under mu.Lock()
func (rf *Raft) applyLogEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMst := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		fmt.Printf("Raft %d term %d apply %d\n", rf.me, rf.currentTerm, applyMst.CommandIndex)
		rf.applyCh <- applyMst
	}
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
	if rf.killed() {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	fmt.Printf("Raft %d term %d receive command %v\n", rf.me, rf.currentTerm, command)

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	rf.printLogInfo()

	rf.startReplication(len(rf.log) - 1)

	return len(rf.log) - 1, rf.currentTerm, true
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

func (rf *Raft) startHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		fmt.Printf("Raft %d term %d starts heartbeat\n", rf.me, rf.currentTerm)

		rf.mu.Unlock()

		for i := range rf.peers {
			if i != rf.me {
				go func(server int) {
					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[server] - 1
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  rf.log[prevLogIndex].Term,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock()

					if rf.sendAppendEntires(server, &args, &reply) {
						if rf.killed() {
							return
						}

						rf.mu.Lock()
						defer rf.mu.Unlock()

						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.resetElectionTimer <- true // avoiding the immediate start of the election
							rf.persist()
							return
						}

						if rf.state == Leader && !reply.Success { // fast backup
							fmt.Printf("Raft %d term %d start backup to Raft %d\n", rf.me, rf.currentTerm, i)
							rf.printLogInfo()
							nextIndex := prevLogIndex + 1
							if reply.XTerm != -1 {
								termLastIndex := rf.findTermLastIndex(reply.XTerm)
								if termLastIndex == -1 {
									nextIndex = reply.XIndex
								} else {
									nextIndex = termLastIndex + 1
								}
							} else {
								nextIndex -= reply.XLen
							}
							go rf.sendAppendEntiresDealWithReply(server, nextIndex)
						}
					}
				}(i)
			}
		}

		time.Sleep(rf.heartbeatTimeout)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	fmt.Printf("Raft %d term %d starts election\n", rf.me, rf.currentTerm)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	voteCount := 1

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.persist()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					if rf.killed() {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					}

					if reply.VoteGranted && rf.state == Candidate {
						voteCount++
						if voteCount >= (len(rf.peers)+1)/2 {
							fmt.Printf("Raft %d term %d becomes leader\n", rf.me, rf.currentTerm)
							rf.state = Leader
							for i := range rf.nextIndex {
								rf.nextIndex[i] = len(rf.log)
								rf.matchIndex[i] = 0
							}
							go rf.startHeartbeat()
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for !rf.killed() {
		timer.Reset(randomElectionTimeout())

		select {
		case <-timer.C:
			rf.mu.Lock()
			if !rf.killed() && rf.state != Leader {
				go rf.startElection()
			}
			rf.mu.Unlock()
		case <-rf.resetElectionTimer:
			continue
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.cv = sync.NewCond(&rf.mu)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = Follower
	rf.heartbeatTimeout = 150 * time.Millisecond
	rf.requestVoteChan = make(chan bool, len(peers))
	rf.appendEntriesChan = make(chan bool, len(peers))
	rf.resetElectionTimer = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	if len(rf.log) == 0 {
		return true
	}
	lastEntry := rf.log[len(rf.log)-1]
	if lastLogTerm != lastEntry.Term {
		return lastLogTerm > lastEntry.Term
	}
	return lastLogIndex >= len(rf.log)-1
}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) printLogInfo() {
	infos := ""
	for _, entry := range rf.log {
		infos += strconv.Itoa(entry.Term) + " "
	}
	fmt.Printf("Raft %d term %d loginfo %s\n", rf.me, rf.currentTerm, infos)
}
