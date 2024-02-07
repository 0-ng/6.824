package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	OpPUT    = "Put"
	OpAppend = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Op    string `json:"op,omitempty"` // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err `json:"err,omitempty"`
}

type GetArgs struct {
	Key string `json:"key,omitempty"`
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err    `json:"err,omitempty"`
	Value string `json:"value,omitempty"`
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	Term     int `json:"term"`      // leader’s term
	LeaderID int `json:"leader_id"` // so follower can redirect clients
}
