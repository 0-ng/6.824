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
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	if rf.VotedFor != nil {
		e.Encode(*rf.VotedFor)
	} else {
		e.Encode(-1)
	}
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CurrentTerm int
	var VotedFor int
	var Log []Entry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil {
		DPrintf("decode error\n")
	} else {
		rf.CurrentTerm = CurrentTerm
		if VotedFor != -1 {
			rf.VotedFor = &VotedFor
		} else {
			rf.VotedFor = nil
		}
		rf.Log = Log
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
			rf.LeaderID = -1
		}
		reply.Term = rf.CurrentTerm
		rf.persist()
		rf.mu.Unlock()
	}()

	// 1. Reply false if term < currentTerm (§5.1)
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term == rf.CurrentTerm && rf.VotedFor != nil && *rf.VotedFor != args.CandidateID {
		reply.VoteGranted = false
		return
	}
	if args.LastLogIndex < rf.CommitIndex-1 {
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term {
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex+1 < len(rf.Log) {
		reply.VoteGranted = false
		return
	}
	rf.VotedFor = &args.CandidateID
	rf.ElectionTimer = time.Now()

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		if rf.CurrentTerm <= args.Term {
			rf.CurrentTerm = args.Term
			rf.LeaderID = args.LeaderID
		}
		reply.Term = rf.CurrentTerm
		rf.persist()
		rf.mu.Unlock()
	}()
	// 1. Reply false if term < currentTerm (§5.1)
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if len(rf.Log) <= args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
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
			// 4. Append any new entries not already in the log
			rf.Log = append(rf.Log, args.Entries[i])
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	commitIndex := args.PrevLogIndex + len(args.Entries) + 1
	if commitIndex > args.LeaderCommit {
		commitIndex = args.LeaderCommit
	}
	if rf.CommitIndex < commitIndex {
		rf.CommitIndex = commitIndex
	}
	for rf.LastApplied+1 < rf.CommitIndex {
		rf.LastApplied += 1
		rf.ApplyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.LastApplied].Entry,
			CommandIndex: rf.LastApplied,
		}
		DPrintf("[AppendEntries]%v apply [%v:%v] from %v, commitIndex %v\n", rf.me, rf.LastApplied, rf.Log[rf.LastApplied].Entry, args.LeaderID, rf.CommitIndex)
	}
	rf.ElectionTimer = time.Now()
	rf.VotedFor = nil

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
	currentTerm := rf.CurrentTerm
	logLen := len(rf.Log)
	rf.persist()
	rf.mu.Unlock()
	ch := make(chan AppendEntriesEnum, len(rf.peers))
	//  先收到过半回复才return，再commit
	rf.sendAppendEntriesToAllServer(logLen, currentTerm, ch)
	return logLen - 1, term, isLeader
	if !rf.sendAppendEntriesToAllServer(logLen, currentTerm, ch) {
		return logLen - 1, term, isLeader
	}
	go func() {
		rf.mu.Lock()
		defer func() {
			rf.applyLog()
			rf.mu.Unlock()
		}()
		if rf.LeaderID != rf.me {
			return
		}
		if rf.Log[logLen-1].Term != rf.CurrentTerm {
			return
		}
		if logLen > rf.CommitIndex {
			rf.CommitIndex = logLen
		}
	}()
	return logLen - 1, term, isLeader
}

func (rf *Raft) sendAppendEntriesToAllServer(logLen, currentTerm int, ch chan AppendEntriesEnum) bool {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			ch <- rf.syncToServer(server, logLen, currentTerm)
		}(server)
	}
	cnt := 0
	for i := 1; i < len(rf.peers); i++ {
		switch <-ch {
		case AppendEntriesEnumDisconnect:
		case AppendEntriesEnumBiggerTerm:
			cnt = -1
		case AppendEntriesEnumSuccess:
			cnt += 1
		}
		if 2*(cnt+1) >= len(rf.peers) || cnt == -1 {
			break
		}
	}
	if 2*(cnt+1) < len(rf.peers) {
		DPrintf("id=%v lose leader cnt=%v\n", rf.me, cnt)
		rf.mu.Lock()
		rf.LeaderID = -1
		rf.mu.Unlock()
		return false
	}
	return true
}

func (rf *Raft) syncToServer(server, logLen, currentTerm int) AppendEntriesEnum {
	start := time.Now()
	for time.Since(start).Seconds() < 0.5 {
		rf.mu.Lock()
		if rf.CurrentTerm > currentTerm || rf.LeaderID != rf.me {
			rf.mu.Unlock()
			return AppendEntriesEnumBiggerTerm
		}
		id := rf.NextIndex[server]
		if id > logLen {
			rf.mu.Unlock()
			return AppendEntriesEnumSuccess
			//id = logLen
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
		for time.Since(start).Seconds() < 0.5 {
			if !rf.sendAppendEntries(server, args, reply) {
				//time.Sleep(10 * time.Millisecond)
				continue
			}
			if reply.Term > currentTerm {
				rf.mu.Lock()
				if rf.CurrentTerm < reply.Term {
					rf.receiveBiggerTerm(reply.Term)
				}
				rf.mu.Unlock()
				return AppendEntriesEnumBiggerTerm
			} else if reply.Success {
				rf.mu.Lock()
				if rf.NextIndex[server] < logLen {
					rf.NextIndex[server] = logLen
				}
				rf.mu.Unlock()
				return AppendEntriesEnumSuccess
			}
			break
		}
		if time.Since(start).Seconds() >= 0.5 {
			break
		}
		rf.mu.Lock()
		if rf.NextIndex[server] <= 1 {
			rf.mu.Unlock()
			return AppendEntriesEnumDisconnect
		}
		// TODO
		rf.NextIndex[server] -= 1
		//rf.NextIndex[server] = 1
		rf.mu.Unlock()
	}
	return AppendEntriesEnumDisconnect
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
			stop := false
			for !stop {
				switch rf.checkExpiredAndElection() {
				case UnExpired:
					stop = true
				case NotEnoughVote:
					ms := rand.Int63() % 50
					time.Sleep(time.Duration(ms) * time.Millisecond)
				case DisConnect, HaveBiggerTerm:
					ms := rand.Int63() % 80
					time.Sleep(time.Duration(ms) * time.Millisecond)
				case Success:
					rf.heartBeat()
					stop = true
				}
			}
		}
	}
}

func (rf *Raft) heartBeat() {
	ch := make(chan AppendEntriesEnum, len(rf.peers))
	rf.mu.Lock()
	logLen := len(rf.Log)
	currentTerm := rf.CurrentTerm
	rf.mu.Unlock()
	if !rf.sendAppendEntriesToAllServer(logLen, currentTerm, ch) {
		return
	}
	rf.mu.Lock()
	defer func() {
		rf.applyLog()
		rf.mu.Unlock()
	}()
	if rf.LeaderID != rf.me {
		return
	}
	if rf.Log[logLen-1].Term != rf.CurrentTerm {
		return
	}
	if logLen > rf.CommitIndex {
		rf.CommitIndex = logLen
	}

}

func (rf *Raft) expired() bool {
	//return rf.LeaderID == -1 || time.Since(rf.ElectionTimer) > (time.Duration((rand.Int63()%150)+350)*time.Millisecond)
	return time.Since(rf.ElectionTimer) > (time.Duration((rand.Int63()%150)+350) * time.Millisecond)
}

func (rf *Raft) checkExpiredAndElection() ElectionResult {
	rf.mu.Lock()
	if !rf.expired() {
		rf.mu.Unlock()
		return UnExpired
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

	ch := make(chan RequestVoteEnum, len(rf.peers))
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				ch <- RequestVoteEnumDisConnect
				return
			}
			if reply.VoteGranted {
				ch <- RequestVoteEnumGrant
			} else {
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.receiveBiggerTerm(reply.Term)
					ch <- RequestVoteEnumBiggerTerm
				} else {
					ch <- RequestVoteEnumLose
				}
				rf.mu.Unlock()
			}
		}(server)
	}
	rec := 0
	grant := 0
	done := time.Tick(time.Second)
	for i := 1; i < len(rf.peers); i++ {
		v := <-ch
		switch v {
		case RequestVoteEnumGrant:
			grant += 1
			rec += 1
		case RequestVoteEnumLose:
			rec += 1
		case RequestVoteEnumDisConnect:

		case RequestVoteEnumBiggerTerm:
			grant = -1
		}
		select {
		case <-done:
			grant = -1
		default:

		}
		if grant == -1 || 2*(grant+1) >= len(rf.peers) {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ct != rf.CurrentTerm {
		DPrintf("[Election]id=%v, ct != rf.CurrentTerm\n", rf.me)
		return HaveBiggerTerm
	}
	if 2*(rec+1) < len(rf.peers) {
		DPrintf("[Election]id=%v, connection error\n", rf.me)
		rf.CurrentTerm -= 1
		rf.VotedFor = nil
		return DisConnect
	}
	if 2*(grant+1) < len(rf.peers) {
		DPrintf("[Election]id=%v, not enough vote\n", rf.me)
		return NotEnoughVote
	}
	DPrintf("[Election]aha~ id=%v\n", rf.me)
	rf.LeaderID = rf.me
	rf.NextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = rf.CommitIndex
	}
	return Success
}

func (rf *Raft) receiveBiggerTerm(term int) {
	rf.CurrentTerm = term
	rf.VotedFor = nil
	rf.LeaderID = -1
}

// with lock previous
func (rf *Raft) applyLog() {
	for rf.LastApplied+1 < rf.CommitIndex {
		rf.LastApplied += 1
		rf.ApplyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.LastApplied].Entry,
			CommandIndex: rf.LastApplied,
		}
		DPrintf("[applyLog]%v apply [%v:%v], commitIndex %v\n", rf.me, rf.LastApplied, rf.Log[rf.LastApplied].Entry, rf.CommitIndex)
	}
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
	rf.LastApplied = 0
	rf.ApplyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
