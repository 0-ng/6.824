package raft

type AppendEntriesEnum int64

const (
	AppendEntriesEnumDisconnect AppendEntriesEnum = 1
	AppendEntriesEnumBiggerTerm AppendEntriesEnum = 2
	AppendEntriesEnumSuccess    AppendEntriesEnum = 3
	AppendEntriesEnumFail       AppendEntriesEnum = 4
)

type ElectionResult int64

const (
	UnExpired      ElectionResult = 1
	DisConnect     ElectionResult = 2
	NotEnoughVote  ElectionResult = 3
	HaveBiggerTerm ElectionResult = 4
	Success        ElectionResult = 5
)

type RequestVoteEnum int64

const (
	RequestVoteEnumGrant      RequestVoteEnum = 1
	RequestVoteEnumLose       RequestVoteEnum = 2
	RequestVoteEnumDisConnect RequestVoteEnum = 3
	RequestVoteEnumBiggerTerm RequestVoteEnum = 4
)
