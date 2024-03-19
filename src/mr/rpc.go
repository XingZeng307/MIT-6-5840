package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

// Add your RPC definitions here.
type AskArgs struct {
	NodePID int
}

type Reply struct {
	TaskType    string
	TaskIndex   int
	NReducer    int
	InputFiles  []string
	OutputFiles []string
	NodePID     int
}

type WorkerNode struct {
	AssignedTaskIndex int
	AssignedTaskType  string
	Timestamp         time.Time
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
