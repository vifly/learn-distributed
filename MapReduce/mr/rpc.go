package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type WorkType int32

const (
	Map    WorkType = 0
	Reduce WorkType = 1
)

type NullArgs struct {
}

type WorkDoneArgs struct {
	TaskId          int
	WorkType        WorkType
	OutputFilePaths []string
}

type NullReply struct {
}

type GetWorkReply struct {
	WorkType WorkType
	TaskId   int
	FilePath string
	NReduce  int
	IsEnd    bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
