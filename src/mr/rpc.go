package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

// MapReduce RPC definitions

type TaskType int

const (
	InvalidTask TaskType = iota
	MapTask
	ReduceTask
	ExitTask
)

type RegisterWorkerArgs struct {
	WorkerId string
}

type RegisterWorkerResponse struct {
	Successful bool
}

type FetchTaskArgs struct {
	WorkerId string
}

type FetchTaskReply struct {
	TaskId     string
	TaskType   TaskType
	InputFiles []string
}

type CommitTaskArgs struct {
	WorkerId              string
	TaskId                string
	TaskType              TaskType
	MapTaskOutputFiles    map[string][]string
	ReduceTaskOutputFiles []string
}

type CommitTaskReply struct {
	Successful bool
}

type FetchNReduceArgs struct {
}

type FetchNReduceReply struct {
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
