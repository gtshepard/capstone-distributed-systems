package mapreduce

import (
	"container/list"
	"fmt"
	"strconv"
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
		info := &WorkerInfo{}
		info.address = worker
		mr.Workers[worker] = info
		//i need to schedule workers to do map jobs on seperate threads
	}

	fmt.Println("THIS IS A TEST")
	go func() {
		var reply *DoJobReply
		args := &DoJobArgs{}
		args.File = mr.file
		args.JobNumber = 0
		args.NumOtherPhase = mr.nReduce
		args.Operation = "Map"
		args.Master = mr.MasterAddress
		args.Worker = workers[0]

		ok := call(workers[0], "Worker.DoJob", args, &reply)
		if ok {
			myLogger("--", "Map Job "+strconv.Itoa(0)+" Done", "RunMaster()", "master.go")
		} else {
			myLogger("--", "RPC fail!", "RunMaster()", "master.go")
		}
	}()
	mapJobDone := <-mr.MapJobChannel
	fmt.Println("Job " + strconv.Itoa(mapJobDone) + " Done")

	go func() {
		var reply *DoJobReply
		args := &DoJobArgs{}
		args.File = mr.file
		args.JobNumber = 0
		args.NumOtherPhase = mr.nMap
		args.Operation = "Reduce"
		args.Master = mr.MasterAddress
		args.Worker = workers[0]

		ok := call(workers[0], "Worker.DoJob", args, &reply)
		if ok {
			myLogger("--", "Map Job "+strconv.Itoa(0)+" Done", "RunMaster()", "master.go")
		} else {
			myLogger("--", "RPC fail!", "RunMaster()", "master.go")
		}
	}()
	fmt.Println("Job Reduce " + strconv.Itoa(0) + " NOT Done")
	reduceJobDone := <-mr.ReduceJobChannel
	fmt.Println("Job " + strconv.Itoa(reduceJobDone) + " Done")

	//mapChannel := make(chan string, mr.nMap)
	//	reduceChannel := make(chan string, mr.nReduce)

	// for i := 0; i < mr.nMap; i++ {
	// 	var reply *DoJobReply
	// 	args := &DoJobArgs{}
	// 	args.File = mr.file
	// 	args.JobNumber = i
	// 	args.NumOtherPhase = mr.nReduce
	// 	args.Operation = "Map"
	// 	args.Master = mr.MasterAddress
	// 	args.Worker = workers[0]

	// 	go func(jobNumber int, myWorker string) {
	// 		mapChannel <- "started" + strconv.Itoa(jobNumber)
	// 		myLogger("11", "Start Map Job "+strconv.Itoa(jobNumber), "RunMaster()", "master.go")
	// 		ok := call(myWorker, "Worker.DoJob", args, &reply)
	// 		//s <- "sync"
	// 		if ok {
	// 			myLogger("--", "Map Job "+strconv.Itoa(jobNumber)+" Done", "RunMaster()", "master.go")
	// 		} else {
	// 			myLogger("--", "RPC fail!", "RunMaster()", "master.go")
	// 		}
	// 	}(i, workers[0])
	// }

	// for i := 0; i < mr.nReduce; i++ {
	// 	var reply *DoJobReply
	// 	args := &DoJobArgs{}
	// 	args.File = mr.file
	// 	args.JobNumber = i
	// 	args.NumOtherPhase = mr.nMap
	// 	args.Operation = "Reduce"
	// 	args.Master = mr.MasterAddress
	// 	args.Worker = workers[1]

	// 	go func(jobNumber int, myWorker string) {
	// 		myLogger("--", "Start Reduce Job "+strconv.Itoa(jobNumber), "RunMaster()", "master.go")
	// 		ok := call(myWorker, "Worker.DoJob", args, &reply)
	// 		if ok {
	// 			myLogger("--", "Reduce Job "+strconv.Itoa(jobNumber)+" Done", "RunMaster()", "master.go")
	// 		} else {
	// 			myLogger("--", "RPC fail!", "RunMaster()", "master.go")
	// 		}
	// 		reduceChannel <- "started"
	// 		//c <- "started"
	// 	}(i, workers[1])
	// }

	//myLogger("----------|||||||||----------", "WATING FOR VALUE", "RunMaster()", "master.go")
	//<-mapChannel
	//myLogger("----------|||||||||----------", "MAP CHANNEL RECIEVED", "RunMaster()", "master.go")
	//<-reduceChannel
	//myLogger("-----------||||||||||----------", "REDUCE CHANNEL RECIEVED", "RunMaster()", "master.go")
	mr.Merge()

	return mr.KillWorkers()
}
