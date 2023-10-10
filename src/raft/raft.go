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

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
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

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	CurrentTerm   int  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor      *int // candidateId that received vote in current term (or null if none)
	LeaderID      int
	ElectionTimer time.Time

	Log         []Entry  // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	CommitIndex int      // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int      // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	NextIndex   []int    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex  []string // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	ApplyCh     chan ApplyMsg
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.LeaderID == rf.me
	//return term, isleader
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int `json:"term"`         // candidate’s term
	CandidateID int `json:"candidate_id"` // candidate requesting vote

	LastLogIndex int `json:"last_log_index"` // index of candidate’s last log entryindex of candidate’s last log entry
	LastLogTerm  int `json:"last_log_term"`  // term of candidate’s last log entry
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  `json:"term"`         // currentTerm, for candidate to update itself
	VoteGranted bool `json:"vote_granted"` // true means candidate received vote
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term == rf.CurrentTerm && rf.VotedFor != nil && *rf.VotedFor != args.CandidateID {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex+1 < len(rf.Log) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	rf.VotedFor = &args.CandidateID
	rf.ElectionTimer = time.Now()
	rf.CurrentTerm = args.Term

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = true

	return
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
	if ok {
		DPrintf("[RequestVote]%v send to %v, args: %+v, reply: %+v\n", rf.me, server, args, reply)
	} else {
		DPrintf("[RequestVote]%v send to %v not ok\n", rf.me, server)
	}
	return ok
}

type Entry struct {
	Term  int         `json:"term"` // leader’s term
	Entry interface{} `json:"entry"`
}

type AppendEntriesArgs struct {
	Term     int `json:"term"`      // leader’s term
	LeaderID int `json:"leader_id"` // so follower can redirect clients

	PrevLogIndex int     `json:"prev_log_index"` // index of log entry immediately preceding new ones
	PrevLogTerm  int     `json:"prev_log_term"`  // term of prevLogIndex entry
	Entries      []Entry `json:"entries"`        // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     `json:"leader_commit"`  // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`    // currentTerm, for leader to update itself
	Success bool `json:"success"` // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if len(rf.Log) <= args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.ElectionTimer = time.Now()
		if len(rf.Log) <= args.PrevLogIndex {
			DPrintf("%v AppendEntries fail, len(rf.Log) <= args.PrevLogIndex, %v, %v\n", rf.me, len(rf.Log), args.PrevLogIndex)
		} else {
			DPrintf("%v AppendEntries fail, rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm, %v, %v\n", rf.me, rf.Log[args.PrevLogIndex].Term, args.PrevLogTerm)
		}
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1 < len(rf.Log) {
			rf.Log[args.PrevLogIndex+i+1] = args.Entries[i]
		} else {
			//// 4. Append any new entries not already in the log
			rf.Log = append(rf.Log, args.Entries[i])
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	lastNewEntriesIdx := args.PrevLogIndex + len(args.Entries) + 1
	if args.LeaderCommit > rf.CommitIndex {
		from := rf.CommitIndex
		rf.CommitIndex = lastNewEntriesIdx
		if args.LeaderCommit < rf.CommitIndex {
			rf.CommitIndex = args.LeaderCommit
		}
		for i := from; i < rf.CommitIndex; i++ {
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Entry,
				CommandIndex: i,
			}
		}
	}
	rf.CurrentTerm = args.Term
	rf.LeaderID = args.LeaderID
	rf.ElectionTimer = time.Now()
	rf.VotedFor = nil

	reply.Term = rf.CurrentTerm
	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if len(args.Entries) == 0 {
			DPrintf("[HeartBeat]%v send to %v, %+v, %+v\n", rf.me, server, args, reply)
		} else {
			DPrintf("[AppendEntries]%v send to %v, %+v, %+v\n", rf.me, server, args, reply)
		}
	} else {
		if len(args.Entries) == 0 {
			DPrintf("[HeartBeat]%v send to %v not ok, %+v\n", rf.me, server, args)
		} else {
			DPrintf("[AppendEntries]%v send to %v not ok, %+v\n", rf.me, server, args)
		}
	}
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
	index := -1
	// Your code here (2B).
	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	rf.Log = append(rf.Log, Entry{Entry: command, Term: term})
	logLen := len(rf.Log)
	ch := make(chan bool, len(rf.peers))
	rf.mu.Unlock()
	go func() {
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.SyncToServer(server, logLen, ch)
			}(server)
		}
		cnt := 0
		for i := 1; i < len(rf.peers); i++ {
			v := <-ch
			if v {
				cnt += 1
			}
			if (cnt+1)*2 >= len(rf.peers) {
				break
			}
		}
		if (cnt+1)*2 < len(rf.peers) {
			rf.mu.Lock()
			rf.LeaderID = -1
			rf.mu.Unlock()
			DPrintf("%v end with fail\n", rf.me)
			return
		}
		rf.mu.Lock()
		if logLen > rf.CommitIndex {
			for i := rf.CommitIndex; i < logLen; i++ {
				rf.ApplyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[i].Entry,
					CommandIndex: i,
				}
			}
			rf.CommitIndex = logLen
		}
		rf.mu.Unlock()
	}()
	return logLen - 1, term, isLeader
}

func (rf *Raft) SyncToServer(server, logLen int, ch chan bool) {
	ok := false
	start := time.Now()
	for time.Since(start).Seconds() < 1 && !ok {
		rf.mu.Lock()
		id := rf.NextIndex[server]
		if id > logLen {
			id = logLen
		}
		args := &AppendEntriesArgs{
			Term:     rf.CurrentTerm,
			LeaderID: rf.LeaderID,

			PrevLogIndex: id - 1,
			PrevLogTerm:  rf.Log[id-1].Term,
			Entries:      rf.Log[id:logLen],
			LeaderCommit: rf.CommitIndex,
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		start2 := time.Now()
		for time.Since(start2).Seconds() < 0.3 {
			if !rf.sendAppendEntries(server, args, reply) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			// TODO change to follower
			if reply.Success {
				ch <- true
				ok = true
			}
			break
		}
		rf.mu.Lock()
		if rf.NextIndex[server] <= 1 || ok {
			rf.mu.Unlock()
			break
		}
		if !ok {
			rf.NextIndex[server] -= 1
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	if !ok {
		ch <- false
	} else {
		rf.mu.Lock()
		rf.NextIndex[server] = logLen
		rf.mu.Unlock()
	}
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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
	for rf.killed() == false {

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (2A)
		// Check if a leader election should be started.

		_, isLeader := rf.GetState()
		if isLeader {
			rf.heartBeat()
		} else {
			if rf.checkExpiredAndElection() {
				rf.heartBeat()
			}
		}
	}
}

func (rf *Raft) heartBeat() {
	cnt := int32(0)
	ch := make(chan bool, len(rf.peers))
	rf.mu.Lock()
	logLen := len(rf.Log)
	rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.SyncToServer(server, logLen, ch)
		}(server)
	}
	for i := 1; i < len(rf.peers); i++ {
		v := <-ch
		if v {
			cnt += 1
			if int(2*(cnt+1)) >= len(rf.peers) {
				break
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if int(2*(cnt+1)) < len(rf.peers) {
		DPrintf("id=%v lose leader cnt=%v\n", rf.me, cnt)
		rf.LeaderID = -1
	}
}
func (rf *Raft) expired() bool {
	return time.Since(rf.ElectionTimer) > (time.Duration((rand.Int63()%600)+1000) * time.Millisecond)
}

func (rf *Raft) checkExpiredAndElection() bool {
	rf.mu.Lock()
	if !rf.expired() {
		rf.mu.Unlock()
		return false
	}
	rf.CurrentTerm += 1
	ct := rf.CurrentTerm
	rf.VotedFor = &rf.me
	rf.ElectionTimer = time.Now()
	args := &RequestVoteArgs{
		Term:         ct,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.Log) - 1,
		LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
	}
	rf.mu.Unlock()
	DPrintf("[Election]id=%v start an election\n", rf.me)

	cnt := 0
	ch := make(chan int, len(rf.peers))
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				ch <- 2
				return
			}
			if reply.VoteGranted {
				ch <- 1
			} else {
				ch <- 0
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
				}
				rf.mu.Unlock()
			}
		}(server)
	}
	rec := 0
	for i := 1; i < len(rf.peers); i++ {
		v := <-ch
		if v == 1 {
			cnt += 1
		}
		if v != 2 {
			rec += 1
		}
		if 2*(cnt+1) >= len(rf.peers) {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if 2*(rec+1) < len(rf.peers) {
		DPrintf("[Election]id=%v, connection error\n", rf.me)
		rf.CurrentTerm -= 1
		rf.VotedFor = nil
		return false
	}
	if 2*(cnt+1) >= len(rf.peers) {
		if ct == rf.CurrentTerm {
			DPrintf("[Election]aha~ id=%v\n", rf.me)
			rf.LeaderID = rf.me
			rf.NextIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.NextIndex[i] = rf.CommitIndex
			}
			return true
		}
		DPrintf("[Election]id=%v, ct != rf.CurrentTerm\n", rf.me)
		return false
	}
	//rf.CurrentTerm -= 1
	DPrintf("[Election]id=%v, not enough vote\n", rf.me)
	return false
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.LeaderID = -1
	rf.Log = []Entry{{Term: 0}}
	rf.CommitIndex = len(rf.Log)
	rf.ApplyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
