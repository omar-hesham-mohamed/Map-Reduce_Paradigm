package mapreduce

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

// Master holds all the state that the master needs to keep track of. Of
// particular importance is registerChannel, the channel that notifies the
// master of workers that have gone idle and are in need of new work.
type Master struct {
	sync.Mutex

	address         string
	registerChannel chan string
	doneChannel     chan bool
	workers         []string // protected by the mutex

	// Per-task information
	jobName string   // Name of currently executing job
	files   []string // Input files
	dirName string   // Parent of input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)
	go func() {
		mr.registerChannel <- args.Worker
	}()
	return nil
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.registerChannel = make(chan string)
	mr.doneChannel = make(chan bool)
	return
}

// Sequential runs map and reduce tasks sequentially, waiting for each task to
// complete before scheduling the next.
func Sequential(jobName string, dirName string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	files := getChildrenFiles(dirName)
	mr = newMaster("master")
	mr.dirName = dirName
	go mr.run(jobName, files, nreduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				runMapTask(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				runReduceTask(mr.jobName, i, len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nreduce}
	})
	return
}

// Distributed schedules map and reduce tasks on workers that register with the
// master over RPC.
func Distributed(jobName string, dirName string, nreduce int, master string) (mr *Master) {
	files := getChildrenFiles(dirName)
	mr = newMaster(master)
	mr.startRPCServer()
	mr.dirName = dirName
	go mr.run(jobName, files, nreduce, mr.schedule, func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	})
	return
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each task on workers as they become available. Each map task bins
// its output in a number of bins equal to the given number of reduce tasks.
// Once all the mappers have finished, workers are assigned reduce tasks.
//
// When all tasks have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	os.RemoveAll(outTestPath)
	err := os.Mkdir(outTestPath, 0755)
	if err != nil {
		log.Fatal("master.run: ", err)
	}

	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nreduce

	debug("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	fmt.Println("Starting Map Phase...")
	schedule(mapPhase)
	
	fmt.Println("Starting Reduce Phase...")
	schedule(reducePhase)
	finish()
	mr.merge()

	debug("%s: Map/Reduce task completed\n", mr.address)

	fmt.Println("All tasks completed. Sending shutdown signal.")
	mr.doneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if !ok {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}
