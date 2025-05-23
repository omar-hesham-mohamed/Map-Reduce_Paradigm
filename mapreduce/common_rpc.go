package mapreduce

import (
	"fmt"
	"net/rpc"
)

// What follows are RPC types and methods.
// Field names must start with capital letters, otherwise RPC will break.

// RunTaskArgs holds the arguments that are passed to a worker when a job is
// scheduled on it.
type RunTaskArgs struct {
	JobName    string
	File       string   // input file, only used in map tasks
	Phase      jobPhase // are we in mapPhase or reducePhase?
	TaskNumber int      // this task's index in the current phase

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int
}

// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// call() returns true if the server responded, and false
// if call() was not able to contact the server. in particular,
// reply's contents are valid if and only if call() returned true.
//
// you should assume that call() will time out and return an
// error (false) after a while if it doesn't get a reply from the server.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	// c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
