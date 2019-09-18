package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// for i := 0; i < 2; i++ {
	// 	worker := <-mr.registerChannel
	// 	myLogger("8", "worker: "+worker, "RunMaster()", "master.go")
	// 	var workerInfo WorkerInfo
	// 	workerInfo.address = worker
	// 	mr.Workers[worker] = workerInfo
	// }

	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		myLogger("8", "worker: "+worker, "RunMaster()", "master.go")
		//	var info *WorkerInfo
		info := &WorkerInfo{}
		info.address = worker
		mr.Workers[worker] = info
		//i need to schedule workers to do map jobs on seperate threads
	}

	// go func() {

	// 	var reply *DoJobReply
	// 	args := &DoJobArgs{}
	// 	args.File = mr.file
	// 	args.JobNumber = 0
	// 	args.NumOtherPhase = mr.nReduce
	// 	args.Operation = "Map"
	// 	ok := call(worker, "Worker.DoJob", args, &reply)
	// 	if ok {
	// 		fmt.Println("Success")
	// 	}

	// }()

	return mr.KillWorkers()
}
