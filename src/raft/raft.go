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
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("[Term %d; Commmand]", l.Term)
}

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
	applyCh    chan ApplyMsg
	state      State
	lastActive time.Time

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) String() string {
	if rf.state == Leader {
		return strings.Join([]string{
			fmt.Sprintf("[currentTerm:%d", rf.currentTerm),
			fmt.Sprintf("votedFor:%d", rf.votedFor),
			fmt.Sprintf("len(rf.log):%d", len(rf.log)),
			fmt.Sprintf("commitIndex:%d", rf.commitIndex),
			fmt.Sprintf("nextIndex:%v", rf.nextIndex),
			fmt.Sprintf("matchIndex:%v]", rf.matchIndex),
		}, " ")
	} else {
		return strings.Join([]string{
			fmt.Sprintf("[currentTerm:%d", rf.currentTerm),
			fmt.Sprintf("votedFor:%d", rf.votedFor),
			fmt.Sprintf("len(rf.log):%d", len(rf.log)),
			fmt.Sprintf("commitIndex:%d]", rf.commitIndex),
		}, " ")
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("failed to decode raft persistent state.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"[Term:%d; LeaderID:%d; PrevLogIndex:%d; PrevLogTerm:%d; len(Entries):%d; LeaderCommit:%d]",
		a.Term,
		a.LeaderID,
		a.PrevLogIndex,
		a.PrevLogTerm,
		len(a.Entries),
		a.LeaderCommit,
	)
}

func (a *AppendEntriesReply) String() string {
	return fmt.Sprintf("[Term: %d; Success: %v]", a.Term, a.Success)
}

func IntMin(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// AppendEntries Receviing Heartbeat from a leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// DPrintf("[%d] received heartbeat from %d on term %d", rf.me, args.LeaderID, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[%d] rejects %d because the leader is stale (rf.currentTerm %d > args.Term %d)",
			rf.me, args.LeaderID, rf.currentTerm, args.Term)
		return
	}

	// Candidate term is more recent, follow this leader.
	rf.lastActive = time.Now()
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.votedFor = -1
	reply.Term = args.Term

	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("[%d] received hb, prevLogIndex out of bound, args.prevLogIndex %d >= len(rf.log)(%d)",
			rf.me, args.PrevLogIndex, len(rf.log))
		reply.Success = false
		return
	}

	if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] received hb, prevLogIndex/Term does not agree, delete, args.prevLogIndex %d, args.prevLogTerm(%d) != rf.log[index].Term(%d)",
			rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		rf.log = rf.log[:args.PrevLogIndex] //delete the log in prevLogIndex and after it
		reply.Success = false
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...) // override
	reply.Success = true
	DPrintf("[%d] received hb, agreed on args.PrevLogIndex %d, args.PrevLogTerm %d, override %d entries, len(rf.log) %d",
		rf.me, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), len(rf.log))

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = IntMin(args.LeaderCommit, len(rf.log)-1)
		DPrintf("[%d] follower commitIndex updated to %d", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) OperateLeader() {
	for !rf.killed() {
		rf.mu.Lock()
		term := rf.currentTerm
		isLeader := rf.state == Leader
		commitIndex := rf.commitIndex
		if !isLeader {
			DPrintf("[%d] Term %d no longer a leader stop sending heartbeat", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// Entry replication/heartbeat RPC
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				var prevLogIndex, prevLogTerm int = rf.nextIndex[server] - 1, -1
				var entries []LogEntry = append([]LogEntry{}, rf.log[rf.nextIndex[server]:]...) // Deep copy
				if prevLogIndex != -1 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				DPrintf("[%d] Term %d, len(entries)=%d entries to be stored on %d: prevLogIndex %d prevLogTerm %d nextIndex[%d]=%d, commitIndex %d",
					rf.me, term, len(entries), server, prevLogIndex, prevLogTerm, server, rf.nextIndex[server], commitIndex)
				var args = AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, entries, commitIndex}
				var reply AppendEntriesReply
				rf.mu.Unlock()

				ok := rf.SendAppendEntries(server, &args, &reply)
				if !ok {
					// DPrintf("[%d] cannot send heartbeat to %d", rf.me, server)
					return
				}

				// DPrintf("[%d] sent heartbeat to %d", rf.me, server)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.persist()
				if reply.Term > rf.currentTerm {
					DPrintf("[%d] found a new Term greater than mine (%d > %d), convert to a Follower",
						rf.me, reply.Term, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					return
				}

				if rf.state != Leader || rf.currentTerm != args.Term {
					DPrintf("[%d] is no longer a leader a or Term has changed from prevous Term %d, rf.currentTerm %d",
						rf.me, args.Term, rf.currentTerm)
					return
				}

				if reply.Success {
					rf.matchIndex[server] = prevLogIndex + len(entries) //must not depend on len(rf.log)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					DPrintf("[%d] knows len(entries)=%d has been replicated to server %d at %d, rf %v",
						rf.me, len(entries), server, prevLogIndex+1, rf)
				} else {
					prevIndex := args.PrevLogIndex
					for prevIndex > -1 && rf.log[prevIndex].Term == args.PrevLogTerm {
						prevIndex--
					}
					rf.nextIndex[server] = prevIndex + 1
					DPrintf("[%d] log not replicate to server %d  try earlier logs, rf %v",
						rf.me, server, rf)
				}

				// count commit index
				newCommitIndex := rf.commitIndex
				for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
					count := 0
					for server := 0; server < len(rf.peers); server++ {
						if rf.matchIndex[server] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
						newCommitIndex = N
						break
					}
				}
				if newCommitIndex > rf.commitIndex {
					DPrintf("[%d] update leader commitIndex from %d to %d",
						rf.me, rf.commitIndex, newCommitIndex)
					rf.commitIndex = newCommitIndex
				}
			}(i)
		}

		heartbeatTimeout := time.Duration(100)
		time.Sleep(heartbeatTimeout * time.Millisecond) // Heartbeat
	}
	DPrintf("[%d] killed as the leader", rf.me)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf("[Term: %d, CandidateID: %d, LastLogIndex: %d, LastLogTerm: %d]",
		rva.Term, rva.CandidateID, rva.LastLogIndex, rva.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("[Term: %d; VoteGranted: %v]", r.Term, r.VoteGranted)
}

func UptoDate(LastLogIndex int, Term int, PeerLastLogIndex int, PeerTerm int) bool {
	// at least as up to date as myself
	// LastLogIndex: 1-based
	return PeerTerm > Term ||
		(PeerTerm == Term && PeerLastLogIndex >= LastLogIndex)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("<-- [%d] done processed vote request from %d. rf %v, request.reply %v",
		rf.me, args.CandidateID, rf, reply)

	DPrintf("--> [%d] on term %d receive vote request from %d. rf %v, request.args %v",
		rf.me, rf.currentTerm, args.CandidateID, rf, args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d] rejects %d, because it has a stale term. currentTerm: %d > candidate.Term: %d.",
			rf.me, args.CandidateID, rf.currentTerm, args.Term)
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		// voted for some other candidate on the same term
		DPrintf("[%d] does not grant vote to %d, because it has voted from other candidate(%d).",
			rf.me, args.CandidateID, rf.votedFor)
		reply.VoteGranted = false
		rf.currentTerm = args.Term
		reply.Term = args.Term
		return
	}

	var myLastIndex, myLastTerm int = len(rf.log) - 1, -1
	if myLastIndex != -1 {
		myLastTerm = rf.log[myLastIndex].Term
	}
	if !UptoDate(myLastIndex, myLastTerm, args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[%d] does not vote for %d because its log is not up to date. "+
			"currentTerm %d, rf.Lastindex %d, rf.lastTerm %d, candidate.LastIndex %d, candidate.LastTerm %d",
			rf.me, args.CandidateID, rf.currentTerm, myLastIndex, myLastTerm, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("[%d] %v realizes that the candidate %d has higher term(%d > %d), convert to a follower.",
			rf.me, rf.state, args.CandidateID, args.Term, rf.currentTerm)
		rf.state = Follower
	}

	rf.votedFor = args.CandidateID
	rf.currentTerm = args.Term
	rf.lastActive = time.Now()
	reply.VoteGranted = true
	reply.Term = args.Term
	DPrintf("[%d] grant vote to %d; reset timer; rf.currentTerm %d, candidate.Term %d, rf.votedFor %d.",
		rf.me, args.CandidateID, rf.currentTerm, args.Term, rf.votedFor)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
func (rf *Raft) Start(command interface{}) (int /*index*/, int /*term*/, bool /*isLeader*/) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.persist()
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		DPrintf("[%d] entry(%v) appended to the leader's log at index %d, len(rf.log)=%d",
			rf.me, command, index, len(rf.log))
		return index + 1, term, isLeader
	}

	//DPrintf("[%d] is not the Leader, do not start the agreement", rf.me)
	return index, term, isLeader
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

func (rf *Raft) KickoffElection() {
	rf.mu.Lock()
	rf.lastActive = time.Now()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("[%d] Term %d start a new election rf %v", rf.me, rf.currentTerm, rf)
	term := rf.currentTerm
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	count := 1
	finished := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			var lastLogIndex, lastLogTerm int = len(rf.log) - 1, -1
			if lastLogIndex != -1 {
				lastLogTerm = rf.log[lastLogIndex].Term
			}
			var args = RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}
			var reply RequestVoteReply
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			if !ok {
				DPrintf("[%d] Term %d, failed to request vote from %d for term %d",
					rf.me, rf.currentTerm, server, term)
				rf.mu.Unlock()
				return
			}
			DPrintf("[%d] Term %d, send requestvote to %d for term %d",
				rf.me, rf.currentTerm, server, term)

			if reply.VoteGranted {
				count++
			}
			finished++
			cond.Broadcast()
			if reply.Term > rf.currentTerm {
				DPrintf("[%d] follower %d term higher(%d > %d), convert from to a Follower",
					rf.me, server, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
		}(i)
	}

	rf.mu.Lock()
	for count < len(rf.peers)/2+1 && finished != len(rf.peers) {
		cond.Wait()
	}

	DPrintf("[%d] count: %d, finished: %d, state: %s", rf.me, count, finished, rf.state)

	if count > len(rf.peers)/2 && (term == rf.currentTerm) && (rf.state == Candidate) {
		rf.state = Leader
		DPrintf("[%d] elected on term %d. %v", rf.me, rf.currentTerm, rf)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = -1
		}
		go rf.OperateLeader()
	} else {
		DPrintf("[%d] lost on term %d, rf.currentTerm %d", rf.me, term, rf.currentTerm)
	}

	rf.mu.Unlock()
}

func (rf *Raft) LeaderElection() {
	for !rf.killed() {
		startTime := time.Now()
		electionTimeout := 500 + (rand.Intn(500))
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastActive.Before(startTime) {
			if rf.state != Leader {
				go rf.KickoffElection()
			}
		}
		rf.mu.Unlock()
	}
	DPrintf("[%d] killed, stop leader election.", rf.me)
}

func (rf *Raft) ApplyLoop() {
	for !rf.killed() {
		applyTimeout := time.Duration(10)
		time.Sleep(applyTimeout * time.Millisecond)

		for {
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()

			rf.mu.Lock()
			commandIndex := rf.lastApplied + 1
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[commandIndex].Command,
				CommandIndex: commandIndex + 1,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			DPrintf("[%d] applied log at index: %d, commitIndex %d",
				rf.me, commandIndex, rf.commitIndex)
			rf.lastApplied = commandIndex
			rf.mu.Unlock()
		}
	}
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastActive = time.Now()
	rf.commitIndex = -1
	rf.applyCh = applyCh
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.LeaderElection()
	go rf.ApplyLoop()

	return rf
}
