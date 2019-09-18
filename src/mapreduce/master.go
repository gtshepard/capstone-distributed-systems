package mapreduce

import (
	"container/list"
	"fmt"
	"strconv"
	"sync"
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

	var workers [2]string
	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		myLogger("8", "worker: "+worker, "RunMaster()", "master.go")
		workers[i] = worker
		//	var info *WorkerInfo
		info := &WorkerInfo{}
		info.address = worker
		mr.Workers[worker] = info
		//i need to schedule workers to do map jobs on seperate threads
	}

	myLogger("9", "Before WG: ", "RunMaster()", "master.go")
	var wg sync.WaitGroup
	wg.Add(2)
	myLogger("10", "AfterWG ", "RunMaster()", "master.go")
	mapChannel := make(chan string)
	//reduceChannel := make(chan string)

	go func(myWorker string, c chan string) {
		myLogger("11", "GO FUNC 1 ", "RunMaster()", "master.go")
		for i := 0; i < mr.nMap; i++ {
			var reply *DoJobReply
			args := &DoJobArgs{}
			args.File = mr.file
			args.JobNumber = 0
			args.NumOtherPhase = mr.nReduce
			args.Operation = "Map"
			myLogger("12", "GO FUNC 1 - AFTER ARGS ", "RunMaster()", "master.go")
			ok := call(myWorker, "Worker.DoJob", args, &reply)

			myLogger("15", "GO FUNC 1 - AFTER RPC ", "RunMaster()", "master.go")
			if ok {
				myLogger("A", "Map Job "+strconv.Itoa(i)+" Done", "RunMaster()", "master.go")
			} else {
				myLogger("X", "RPC fail!", "RunMaster()", "master.go")
			}
		}
		c <- "ALL MAP JOBS DONE"
	}(workers[0], mapChannel)

	// go func(myWorker string, c chan string) {
	// 	myLogger("11", "GO FUNC 2 ", "RunMaster()", "master.go")
	// 	for i := 0; i < mr.nReduce; i++ {
	// 		var reply *DoJobReply
	// 		args := &DoJobArgs{}
	// 		args.File = mr.file
	// 		args.JobNumber = 1
	// 		args.NumOtherPhase = mr.nMap
	// 		args.Operation = "Reduce"
	// 		myLogger("12", "GO FUNC 2 ", "RunMaster()", "master.go")
	// 		ok := call(myWorker, "Worker.DoJob", args, &reply)
	// 		if ok {
	// 			myLogger("B", "Reduce Job "+strconv.Itoa(i)+" Done", "RunMaster()", "master.go")
	// 		} else {
	// 			myLogger("Y", "RPC fail!", "RunMaster()", "master.go")
	// 		}
	// 	}
	// 	c <- "ALL REDUCE JOBS DONE"
	// }(workers[1], reduceChannel)

	mapMsg := <-mapChannel
	myLogger("18", "Map Channel Recieve: "+mapMsg, "RunMaster()", "master.go")
	//reduceMsg := <-reduceChannel
	//myLogger("19", "Reduce Channel Recieve: "+reduceMsg, "RunMaster()", "master.go")
	// myLogger("C", "I AM WAITING", "RunMaster()", "master.go")
	myLogger("D", "MAP AND REDUCE HAVE RUN!", "RunMaster()", "master.go")
	mr.Merge()

	//you have 2 workers you want them to be busy as msuch as possible
	//as soon as a worker is ready , you want to give a worker a job
	//this
	return mr.KillWorkers()
}
