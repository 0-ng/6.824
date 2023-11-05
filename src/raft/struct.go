package raft

import (
	"6.5840/labrpc"
	"sync"
	"time"
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

	Log         []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	CommitIndex int     // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int     // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	NextIndex   []int   // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex  []int   // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	ApplyCh     chan ApplyMsg
	ApplyCh2    chan ApplyMsg
	//ApplyLock   sync.Mutex
	//ApplyCond   *sync.Cond
	//ApplyQueue  []ApplyMsg

	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
	SnapshotList      []byte
}

type InstallSnapshotArgs struct {
	Term              int    `json:"term,omitempty"`                // leader’s term
	LeaderID          int    `json:"leader_id,omitempty"`           // so follower can redirect clients
	LastIncludedIndex int    `json:"last_included_index,omitempty"` // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    `json:"last_included_term,omitempty"`  // term of lastIncludedIndex
	Offset            int    `json:"offset,omitempty"`              // byte offset where chunk is positioned in the snapshot file
	Data              []byte `json:"data,omitempty"`                // raw bytes of the snapshot chunk, starting at offset
	Done              bool   `json:"done,omitempty"`                // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int `json:"term,omitempty"` // currentTerm, for leader to update itself
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
