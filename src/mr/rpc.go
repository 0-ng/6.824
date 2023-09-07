package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
}

type TaskType int64

const (
	Waiting TaskType = 0
	Map     TaskType = 1
	Reduce  TaskType = 2
	Done    TaskType = 3
)

type GetTaskReply struct {
	FileName       string
	ReduceKey      string
	ReduceKeyFiles []string
	TaskType       TaskType
}

type DoneTaskArgs struct {
	FileName string
	KeyFiles []struct {
		Key      string
		FileName string
	}

	ReduceKey string

	TaskType TaskType
}

type DoneTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
