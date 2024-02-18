package kvraft

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
)

const (
	OpPUT    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	RequestID string `json:"request_id,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Op        string `json:"op,omitempty"` // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err `json:"err,omitempty"`
}

type GetArgs struct {
	RequestID string `json:"request_id,omitempty"`
	Key       string `json:"key,omitempty"`
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
