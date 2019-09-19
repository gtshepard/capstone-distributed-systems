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

	var workers []*RegisterArgs

	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		worker.isIdle = true
		myLogger("8", "worker: ", "RunMaster()", "master.go")
		workers = append(workers, worker)
		info := &WorkerInfo{}
		info.address = worker.Worker
		mr.Workers[worker.Worker] = info
		//i need to schedule workers to do map jobs on seperate threads
	}
	fmt.Println("Hello")
	// fmt.Println("THIS IS A TEST")
	// go func() {
	// 	var reply *DoJobReply
	// 	args := &DoJobArgs{}
	// 	args.File = mr.file
	// 	args.JobNumber = 0
	// 	args.NumOtherPhase = mr.nReduce
	// 	args.Operation = "Map"
	// 	args.Master = mr.MasterAddress
	// 	args.Worker = workers[0]

	// 	ok := call(workers[0], "Worker.DoJob", args, &reply)
	// 	if ok {
	// 		myLogger("--", "Map Job "+strconv.Itoa(0)+" Done", "RunMaster()", "master.go")
	// 	} else {
	// 		myLogger("--", "RPC fail!", "RunMaster()", "master.go")
	// 	}
	// }()
	// mapJobDone := <-mr.MapJobChannel
	// fmt.Println("Job " + strconv.Itoa(mapJobDone) + " Done")

	//go func() {
	// 	var reply *DoJobReply
	// 	args := &DoJobArgs{}
	// 	args.File = mr.file
	// 	args.JobNumber = 0
	// 	args.NumOtherPhase = mr.nMap
	// 	args.Operation = "Reduce"
	// 	args.Master = mr.MasterAddress
	// 	args.Worker = workers[0]

	// 	ok := call(workers[0], "Worker.DoJob", args, &reply)
	// 	if ok {
	// 		myLogger("--", "Map Job "+strconv.Itoa(0)+" Done", "RunMaster()", "master.go")
	// 	} else {
	// 		myLogger("--", "RPC fail!", "RunMaster()", "master.go")
	// 	}
	// }()
	// fmt.Println("Job Reduce " + strconv.Itoa(0) + " NOT Done")
	// reduceJobDone := <-mr.ReduceJobChannel
	// fmt.Println("Job " + strconv.Itoa(reduceJobDone) + " Done")

	//one thing to note reduce job kept failing when only one map job  was scheduled

	// master must schedule all jobs and wait for there completion
	// master can only schedule jobs to avialible workers
	// there are 2 different job types Map or Reduce. there will be M map jobs and R reduce jobs
	// the set of availible workers is accessable as an array. each worker has an IsIdle property
	// we search the availible set of workers  for an idle worker.
	// if we find an idle worker, we set the workers isIdle flag to false and
	// we schedule a job by starting another thread of execution()
	// and make an RPC call to a worker to start the job
	// we start another thread of exeuction so master can continue scheduling while the other workers are running
	// if there is no idle worker, master must wait for one of the non-idle workers to complete a job.
	// master waits to recieve a message from the job completion channel then schedules the job to the worker
	// that just completed it job.
	// example of process described above: same logic can be used for both map and reduce(just sub in ReduceJob for the loop)
	// for i in MapJob:
	//     if isAvailbleWorker:
	//		  idleWoker = getIdleWorker()
	//        go schedule(idleWorker, job)
	//	   else:
	//		  idleWorker := <-signalMapCompletedJob
	//	      append(mapSlice, idleWorker)
	//		  go scheudle(idleWorker, job)
	//
	// recall master must wait until all jobs (both map and reduce) have completed, and then merge the results
	// since each job has been scheduled on seperate threads of execution we have no way of knowing when these jobs
	// will complete, so it is not enough to let these loops expire. yet we must garuntee that all jobs finish, becuase master will just continue its thread of exexution
	// we need another mechanism which bring us to a point of synchronization, a garuntee that we know the order of what will happen next
	// in our case all jobs will finish, then we proceed  on
	// this can be done waiting until we collect nMap completed jobs from the mapCompletedJob channel, and
	// waiting until we collect nReduce completed jobs from the reduceCompletedJob channel
	// an example of this would be as follows
	//
	// while nMap != len(mapSlice):
	//   mapJobDone := <-signalMapCompletedJob
	//   append(mapSlice, mapJobDone)
	//
	// while nReduce != len(reduceSlice):
	//   reduceJobDone := <-signalReduceCompletedJob
	//   append(reduceSlice, reduceJobDone)
	//
	// merge()
	//
	// master shutsdown all workers

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
