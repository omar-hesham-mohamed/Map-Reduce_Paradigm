package mapreduce

import (
	"sync"
	"fmt"
)

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	// ToDo: Complete this function. See the description in the assignment.

	workChan := mr.registerChannel
	var wg sync.WaitGroup
	wg.Add(ntasks)

	done := 0

	fmt.Println("In phase: ", phase)

	for i := 0; i < ntasks; i++ {
		// worker := <- workChan
		fmt.Println("Beginning task: ", i)
		worker, ok := <-workChan
		fmt.Println("Assigned worker to task: ", i, worker)
		if !ok {
			fmt.Println("No workers available, exiting...")
			return
		}

		args := RunTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: numOtherPhase,
		}

		go func(worker string, args RunTaskArgs, taskNo int) {
			// defer wg.Done()
			defer func() {
				fmt.Printf("Task %d completed. Marking as done.\n", taskNo)
				done += 1
				// wg.Done() // deadlockkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
			}()

			// wee need to loop on task until it is completed. if worker fails, get new worker from channel
			for {
				fmt.Printf("Trying to assign %v task %d to worker\n", phase, taskNo)
				comp := call(worker, "Worker.RunTask", &args, new(struct{}))

				if comp {
					fmt.Printf("%v task %d completed successfully on worker\n", phase, taskNo)
					wg.Done()
					workChan <- worker //return worker
					// select { 
					// case workChan <- worker:
					// 	fmt.Println("Worker returned to queue:", taskNo)
					// default:
					// 	fmt.Println("Worker queue is full, skipping return", taskNo)
					// } //on last tasks, no one was requesting workers so assigning workers to channel was blocking

					return
				}

				fmt.Printf("Worker failed for task %d! Retrying with a new worker...\n", taskNo)
				worker = <-workChan // get new idle worker
			}
		}(worker, args, i)

	}

	fmt.Println("Done: ", done)

	wg.Wait()

	fmt.Println("All tasks for", phase, "phase are completed. Moving to next phase.")
}

// type RunTaskArgs struct {
// 	JobName    string
// 	File       string   
// 	Phase      jobPhase 
// 	TaskNumber int   
// 	NumOtherPhase int
// }
