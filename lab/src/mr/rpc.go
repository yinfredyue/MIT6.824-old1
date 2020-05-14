package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type Args struct {
	TaskType int // 0 for map task, 1 for reduce task
	Index    int // Index of done task. Index is set to -1 when requesting task
}

// Worker would use this information to know the task to do.
type Reply struct {
	// The fields need to exported, otherwise you get error:
	// rpc: gob error encoding body: gob: type mr.Reply has no exported fields
	TaskType  int    // 0 for map task, 1 for reduce task, 2 for exit
	Index     int    // task index for map task or reduce task, depending on TaskType
	InputFile string // input file
	R         int    // number of reduce tasks
	M         int    // number of map tasks
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

///////////////////// Example Code //////////////////////////////////////

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }
