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
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func (rf *Raft) test1(skip int, tp string) {
	callerFuncName := "unknown"

	pc, file, line, ok := runtime.Caller(skip)

	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			callerFuncName = fn.Name()
		}
		fmt.Printf("%v%v caller:%s, file:%s, line:%d\n", rf.me, tp, callerFuncName, file, line)
	}
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
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

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.Unlock()
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

	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.SnapshotList)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("bootstrap without any state")
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CurrentTerm int
	var VotedFor int
	var Log []Entry
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&LastIncludedIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil {
		DPrintf("decode error\n")
	} else {
		rf.CurrentTerm = CurrentTerm
		if VotedFor != -1 {
			rf.VotedFor = &VotedFor
		} else {
			rf.VotedFor = nil
		}
		rf.Log = Log
		rf.LastIncludedIndex = LastIncludedIndex
		rf.LastIncludedTerm = LastIncludedTerm
		rf.LastApplied = rf.LastIncludedIndex
		rf.CommitIndex = rf.LastIncludedIndex + 1
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// TODO
	rf.Lock()
	defer func() {
		rf.persist()
		rf.Unlock()
	}()
	if index < rf.LastIncludedIndex {
		return
	}
	//0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
	//0 10 11 12 13 14 15 16 17 18 19 20
	//0 20
	//9 19
	//19 -9 10
	lastLog := rf.Log[index-rf.LastIncludedIndex]
	DPrintf("[Snapshot]%v pre log %v\n", rf.me, rf.Log)
	rf.Log = append(rf.Log[:1], rf.Log[index+1-rf.LastIncludedIndex:]...)
	DPrintf("[Snapshot]%v after log %v\n", rf.me, rf.Log)
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = lastLog.Term
	rf.SnapshotList = snapshot
	DPrintf("[Snapshot]%v save snapshot to index %v\n", rf.me, index)

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer func() {
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
			rf.LeaderID = args.LeaderID
		}
		reply.Term = rf.CurrentTerm
		rf.persist()
		rf.Unlock()
	}()
	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	// 2. Create new snapshot file if first chunk (offset is 0)

	// 3. Write data into snapshot file at given offset

	// 4. Reply and wait for more data chunks if done is false
	if !args.Done {
		return
	}

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	}

	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	if args.LastIncludedIndex-rf.LastIncludedIndex < len(rf.Log) {
		return
	}
	// 7. Discard the entire log
	rf.SnapshotList = args.Data
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.Log = rf.Log[:1]

	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.SnapshotList,
		SnapshotTerm:  rf.LastIncludedTerm,
		SnapshotIndex: rf.LastIncludedIndex,
	}
	rf.CommitIndex = rf.LastIncludedIndex + 1
	rf.LastApplied = rf.LastIncludedIndex
	DPrintf("[InstallSnapshot]%v set LastApplied=%v, CommitIndex=%v\n", rf.me, rf.LastApplied, rf.CommitIndex)
	rf.Unlock()
	rf.ApplyCh <- msg
	rf.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		DPrintf("[InstallSnapshot]%v send to %v, args: %+v, reply: %+v\n", rf.me, server, args, reply)
	} else {
		DPrintf("[InstallSnapshot]%v send to %v not ok\n", rf.me, server)
	}
	return ok
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer func() {
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
			rf.LeaderID = -1
		}
		reply.Term = rf.CurrentTerm
		rf.persist()
		rf.Unlock()
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
	myTerm := rf.LastIncludedTerm
	if len(rf.Log) > 1 {
		myTerm = rf.Log[len(rf.Log)-1].Term
	}
	if args.LastLogTerm < myTerm {
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm == myTerm && args.LastLogIndex < len(rf.Log)-1+rf.LastIncludedIndex {
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
	rf.Lock()
	defer func() {
		if rf.CurrentTerm <= args.Term {
			rf.CurrentTerm = args.Term
			rf.LeaderID = args.LeaderID
		}
		reply.Term = rf.CurrentTerm
		rf.persist()
		rf.applyLog()
		rf.Unlock()
	}()
	// 1. Reply false if term < currentTerm (§5.1)
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		return
	}
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.Success = true
		return
	}
	if args.PrevLogIndex == rf.LastIncludedIndex {
		if args.PrevLogTerm != rf.LastIncludedTerm {
			reply.Success = false
			return
		}
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.LastIncludedIndex+len(rf.Log) <= args.PrevLogIndex {
		reply.Success = false
		rf.ElectionTimer = time.Now()
		DPrintf("%v AppendEntries fail, len(rf.Log) <= args.PrevLogIndex, %v, %v\n", rf.me, rf.LastIncludedIndex+len(rf.Log), args.PrevLogIndex)
		return
	}
	if args.PrevLogIndex-rf.LastIncludedIndex > 0 && rf.Log[args.PrevLogIndex-rf.LastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.ElectionTimer = time.Now()
		DPrintf("%v AppendEntries fail, rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm, %v, %v\n", rf.me, rf.Log[args.PrevLogIndex-rf.LastIncludedIndex].Term, args.PrevLogTerm)
		return

	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1-rf.LastIncludedIndex < len(rf.Log) {
			if rf.Log[args.PrevLogIndex+i+1-rf.LastIncludedIndex].Term != args.Entries[i].Term {
				rf.Log = rf.Log[:args.PrevLogIndex+i+1-rf.LastIncludedIndex]
				rf.Log = append(rf.Log, args.Entries[i:]...)
				break
			}
			rf.Log[args.PrevLogIndex+i+1-rf.LastIncludedIndex] = args.Entries[i]
		} else {
			// 4. Append any new entries not already in the log
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
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
	rf.Lock()
	rf.Log = append(rf.Log, Entry{Entry: command, Term: term})
	currentTerm := rf.CurrentTerm
	logIdx := len(rf.Log) + rf.LastIncludedIndex
	rf.persist()
	rf.Unlock()
	ch := make(chan AppendEntriesEnum, len(rf.peers))
	//  先收到过半回复才return，再commit
	if !rf.sendAppendEntriesToAllServer(logIdx, currentTerm, ch) {
		return logIdx - 1, term, isLeader
	}
	rf.checkCommitIndexAndUpdate(logIdx)
	return logIdx - 1, term, isLeader
}

func (rf *Raft) sendAppendEntriesToAllServer(logIdx, currentTerm int, ch chan AppendEntriesEnum) bool {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			ch <- rf.syncToServer(server, logIdx, currentTerm)
		}(server)
	}
	done := time.Tick(time.Second)
	ok := false
	cnt := 0
	for !ok {
		select {
		case v := <-ch:
			switch v {
			case AppendEntriesEnumDisconnect:
			case AppendEntriesEnumBiggerTerm:
				cnt = -1
			case AppendEntriesEnumSuccess:
				cnt += 1
			}
			if 2*(cnt+1) >= len(rf.peers) || cnt == -1 {
				break
			}
		case <-done:
			ok = true
		default:
			if cnt == -1 || 2*(cnt+1) >= len(rf.peers) {
				ok = true
			}
		}
	}
	if 2*(cnt+1) < len(rf.peers) {
		DPrintf("id=%v lose leader cnt=%v\n", rf.me, cnt)
		rf.Lock()
		rf.LeaderID = -1
		rf.Unlock()
		return false
	}
	return true
}

func (rf *Raft) syncToServer(server, logIdx, currentTerm int) AppendEntriesEnum {
	start := time.Now()
	for time.Since(start).Seconds() < 0.5 {
		rf.Lock()
		if rf.CurrentTerm > currentTerm || rf.LeaderID != rf.me {
			rf.Unlock()
			return AppendEntriesEnumBiggerTerm
		}
		id := rf.NextIndex[server]
		if id > logIdx {
			rf.Unlock()
			DPrintf("[syncToServer]id > logIdx\n")
			return AppendEntriesEnumSuccess
		}
		if id <= rf.LastIncludedIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.CurrentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.LastIncludedIndex,
				LastIncludedTerm:  rf.LastIncludedTerm,
				Offset:            0,
				Data:              rf.SnapshotList,
				Done:              true,
			}
			reply := &InstallSnapshotReply{}
			rf.Unlock()
			if !rf.sendInstallSnapshot(server, args, reply) {
				DPrintf("[syncToServer]%v->%v sendInstallSnapshot not ok\n", rf.me, server)
				return AppendEntriesEnumDisconnect
			}
			if reply.Term > currentTerm {
				rf.Lock()
				if rf.CurrentTerm < reply.Term {
					rf.receiveBiggerTerm(reply.Term)
				}
				rf.Unlock()
				return AppendEntriesEnumBiggerTerm
			}
			rf.Lock()
			if rf.NextIndex[server] < args.LastIncludedIndex+1 {
				rf.NextIndex[server] = args.LastIncludedIndex + 1
			}
			if rf.MatchIndex[server] < args.LastIncludedIndex+1 {
				rf.MatchIndex[server] = args.LastIncludedIndex + 1
			}
			rf.Unlock()
			return AppendEntriesEnumDisconnect
		}
		if logIdx <= rf.LastIncludedIndex {
			rf.Unlock()
			DPrintf("[syncToServer]logIdx <= rf.LastIncludedIndex\n")
			return AppendEntriesEnumSuccess
		}

		prevLogIndex := id - 1
		prevLogTerm := rf.LastIncludedTerm
		if prevLogIndex > rf.LastIncludedIndex {
			prevLogTerm = rf.Log[prevLogIndex-rf.LastIncludedIndex].Term
		}
		args := &AppendEntriesArgs{
			Term:     rf.CurrentTerm,
			LeaderID: rf.LeaderID,

			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.Log[id-rf.LastIncludedIndex : logIdx-rf.LastIncludedIndex], // TODO lock前压缩了怎么办
			LeaderCommit: rf.CommitIndex,
		}
		rf.Unlock()

		reply := &AppendEntriesReply{}
		for time.Since(start).Seconds() < 0.5 {
			if !rf.sendAppendEntries(server, args, reply) {
				continue
			}
			if reply.Term > currentTerm {
				rf.Lock()
				if rf.CurrentTerm < reply.Term {
					rf.receiveBiggerTerm(reply.Term)
				}
				rf.Unlock()
				return AppendEntriesEnumBiggerTerm
			} else if reply.Success {
				rf.Lock()
				if rf.NextIndex[server] < logIdx {
					rf.NextIndex[server] = logIdx
				}
				if rf.MatchIndex[server] < logIdx {
					rf.MatchIndex[server] = logIdx
				}
				rf.Unlock()
				return AppendEntriesEnumSuccess
			}
			break
		}
		if time.Since(start).Seconds() >= 0.5 {
			break
		}
		rf.Lock()
		if rf.NextIndex[server] <= 1 {
			rf.Unlock()
			return AppendEntriesEnumDisconnect
		}
		// TODO
		rf.NextIndex[server] -= 1
		//rf.NextIndex[server] = 1
		rf.Unlock()
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
	DPrintf("[heartBeat]%v begin to acquire lock for heartbeat\n", rf.me)
	rf.Lock()
	logLen := len(rf.Log) + rf.LastIncludedIndex
	currentTerm := rf.CurrentTerm
	rf.Unlock()
	if !rf.sendAppendEntriesToAllServer(logLen, currentTerm, ch) {
		return
	}
	rf.checkCommitIndexAndUpdate(logLen)
}

func (rf *Raft) checkCommitIndexAndUpdate(logIdx int) {
	go func() {
		rf.Lock()
		defer func() {
			rf.persist()
			rf.applyLog()
			rf.Unlock()
		}()
		if logIdx-1 <= rf.LastIncludedIndex {
			return
		}
		if rf.LeaderID != rf.me {
			return
		}
		if rf.Log[logIdx-1-rf.LastIncludedIndex].Term != rf.CurrentTerm {
			return
		}
		if logIdx > rf.CommitIndex {
			rf.CommitIndex = logIdx
		}
	}()
}

func (rf *Raft) expired() bool {
	//return rf.LeaderID == -1 || time.Since(rf.ElectionTimer) > (time.Duration((rand.Int63()%150)+350)*time.Millisecond)
	return time.Since(rf.ElectionTimer) > (time.Duration((rand.Int63()%150)+350) * time.Millisecond)
}

func (rf *Raft) checkExpiredAndElection() ElectionResult {
	rf.Lock()
	if !rf.expired() {
		rf.Unlock()
		return UnExpired
	}
	rf.CurrentTerm += 1
	ct := rf.CurrentTerm
	rf.VotedFor = &rf.me
	rf.ElectionTimer = time.Now()
	LastLogIndex := rf.LastIncludedIndex
	LastLogTerm := rf.LastIncludedTerm
	if len(rf.Log) > 1 {
		LastLogIndex += len(rf.Log) - 1
		LastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	args := &RequestVoteArgs{
		Term:         ct,
		CandidateID:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}
	rf.Unlock()
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
				rf.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.receiveBiggerTerm(reply.Term)
					ch <- RequestVoteEnumBiggerTerm
				} else {
					ch <- RequestVoteEnumLose
				}
				rf.Unlock()
			}
		}(server)
	}
	rec := 0
	grant := 0
	done := time.Tick(time.Second)
	ok := false
	for !ok {
		select {
		case v := <-ch:
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
		case <-done:
			ok = true
		default:
			if grant == -1 || 2*(grant+1) >= len(rf.peers) {
				ok = true
			}
		}
	}
	rf.Lock()
	defer rf.Unlock()
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
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = rf.CommitIndex
		rf.MatchIndex[i] = 0
	}
	return Success
}

func (rf *Raft) receiveBiggerTerm(term int) {
	rf.CurrentTerm = term
	rf.VotedFor = nil
	rf.LeaderID = -1
}

func (rf *Raft) applyLogRoutine() {
	for {
		v := <-rf.ApplyCh2
		rf.ApplyCh <- v
	}
}

// with lock previous
func (rf *Raft) applyLog() {
	for rf.LastApplied+1 < rf.CommitIndex {
		rf.LastApplied += 1
		DPrintf("[applyLog]%v begin to apply [%v:%v], commitIndex %v\n", rf.me, rf.LastApplied, rf.Log[rf.LastApplied-rf.LastIncludedIndex].Entry, rf.CommitIndex)
		//msg :=
		rf.ApplyCh2 <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.LastApplied-rf.LastIncludedIndex].Entry,
			CommandIndex: rf.LastApplied,
		}
		//rf.ApplyQueue = append(rf.ApplyQueue, msg)
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
	rf.LastIncludedIndex = 0
	rf.ApplyCh2 = make(chan ApplyMsg, 10086)
	//rf.ApplyCond = sync.NewCond(&rf.ApplyLock)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.SnapshotList = persister.ReadSnapshot()
	DPrintf("[Make]%v restore from persist %+v\n", rf.me, rf)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogRoutine()
	return rf
}
