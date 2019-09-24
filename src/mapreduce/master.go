package mapreduce

import (
	"container/list"
	"fmt"
	"strconv"
)

type WorkerInfo struct {
	Address   string
	IsIdle    bool
	Number    int
	HasFailed bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.Address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.Address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.Address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) AssignJobToIdleWorker(job JobType, jobNumber int, worker string, otherPhase int, c chan string, f chan string) {

	var reply *DoJobReply
	args := &DoJobArgs{}
	args.File = mr.file
	args.JobNumber = jobNumber
	args.NumOtherPhase = otherPhase
	args.Operation = job
	args.Master = mr.MasterAddress
	args.Worker = worker
	ok := call(worker, "Worker.DoJob", args, &reply)

	if ok {
		myLogger("*****************************", "Successful RPC call to worker", "RunMaster()", "master.go")
		//f <- "PIZZA"
	} else {
		myLogger("RM-10", "RPC call to worker failed", "RunMaster()", "master.go")
		f <- "PIZZA"
	}
	c <- worker
}

func (mr *MapReduce) getIdleWorker(workers []*RegisterArgs) (int, string, bool) {
	for i := 0; i < len(workers); i++ {
		if workers[i].isIdle {
			return i, workers[i].Worker, true
		}
	}
	return 0, "", false
}

func (mr *MapReduce) DistributedMap(workers []*RegisterArgs, buffer chan string) {
	for i := 0; i < mr.nMap; i++ {
		index, workerName, isAvailibleWorker := mr.getIdleWorker(workers)
		if isAvailibleWorker {
			workers[index].isIdle = false
			go mr.AssignJobToIdleWorker("Map", i, workerName, mr.nReduce, buffer, buffer)
		} else {
			idleWorker := <-mr.MapJobChannel
			go mr.AssignJobToIdleWorker("Map", i, idleWorker.Worker, mr.nReduce, buffer, buffer)
		}
	}
	<-mr.MapJobChannel
}

func (mr *MapReduce) DistributedReduce(workers []*RegisterArgs, buffer chan string) {
	for i := 0; i < mr.nReduce; i++ {
		index, workerName, isAvailibleWorker := mr.getIdleWorker(workers)
		if isAvailibleWorker {
			workers[index].isIdle = false
			go mr.AssignJobToIdleWorker("Reduce", i, workerName, mr.nMap, buffer, buffer)
		} else {
			idleWorker := <-mr.ReduceJobChannel
			go mr.AssignJobToIdleWorker("Reduce", i, idleWorker, mr.nMap, buffer, buffer)
		}
	}
	<-mr.ReduceJobChannel
}
func (mr *MapReduce) RecieveWorkers() []*RegisterArgs {
	var workers []*RegisterArgs

	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		worker.isIdle = true
		workers = append(workers, worker)
		info := &WorkerInfo{}
		info.Address = worker.Worker
		//mr.Workers[worker.Worker] = info
	}
	return workers
}

func (mr *MapReduce) RemoveFailedWorker(worker string, workerPool []*RegisterArgs) []*RegisterArgs {

	for i := 0; i < len(workerPool); i++ {
		if workerPool[i].Worker == worker {
			workerPool[i] = workerPool[len(workerPool)-1] // Copy last element to index i.
			workerPool[len(workerPool)-1] = nil           // Erase last element (write zero value).
			workerPool = workerPool[:len(workerPool)-1]   //Truncate slice.
		}
	}
	return workerPool
}

func (mr *MapReduce) MakeAllWorkersIdle(workers []*RegisterArgs) {
	for i := 0; i < len(workers); i++ {
		workers[i].isIdle = true
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	//mapSyncChannel := make(chan string, mr.nMap)
	//reduceSyncChannel := make(chan string, mr.nReduce)
	// workers := mr.RecieveWorkers()
	// mr.DistributedMap(workers, mapSyncChannel)
	// mr.MakeAllWorkersIdle(workers)
	// mr.DistributedReduce(workers, reduceSyncChannel)
	//register workers and set them to idle
	myLogger("RM-1", "START OF TEST", "RunMaster()", "master.go")
	var workers []*RegisterArgs

	//find an idle worker
	getIdleWorker := func(s []*RegisterArgs) (int, string, bool) {
		for i := 0; i < len(s); i++ {
			if s[i].isIdle {
				//myLogger("RM-3", "Found Idle Worker", "getIdleWorker", "master.go")
				return i, s[i].Worker, true
			} else {
				//myLogger("RM-3", "No Idle Worker", "RunMaster()", "master.go")
			}
		}
		return 0, "", false
	}

	IsIdleWorker := func() bool {
		for i := 0; i < len(mr.Workers); i++ {
			if mr.Workers[i].IsIdle {
				return true
			}
		}
		return false
	}

	getAnIdleWorker := func() *WorkerInfo {
		for i := 0; i < len(mr.Workers); i++ {
			if mr.Workers[i].IsIdle {
				return mr.Workers[i]
			}
		}
		return nil
	}

	//completedMapJobs := make([]string, 0)
	//completedReduceJobs := make([]string, 0)
	mapSyncChannel := make(chan string, mr.nMap)
	reduceSyncChannel := make(chan string, mr.nReduce)
	workerFailureChannel := make(chan string)
	//var mapIdleWg sync.WaitGroup
	//var mapNonIdleWg sync.WaitGroup
	//schedule all M map jobs
	//jobs can only be scheduled on idle workers

	//select {
	// case worker := <-mr.registerChannel:
	//  worker.isIdle = true
	//  myLogger("RM-2", "register worker: "+worker.Worker, "RunMaster() - register-worker", "master.go")
	//  workers = append(workers, worker)
	//  info := &WorkerInfo{}
	//  info.address = worker.Worker
	//  mr.Workers[worker.Worker] = info
	// default:
	//  myLogger("RM-2", "FAIL TO REGISTER", "RunMaster() - register-worker", "master.go")
	// }

	for i := 0; i < mr.nMap; i++ {

		select {
		case newWorker := <-mr.registerChannel:
			l := len(newWorker.Worker)
			last, err := strconv.Atoi(newWorker.Worker[l-1 : l])
			if err != nil {
				myLogger("*********", "STR ERROR", "RunMaster()", "master.go")
			}
			worker := &WorkerInfo{}
			worker.Address = newWorker.Worker
			worker.IsIdle = true
			worker.Number = last
			myLogger("&&&&&&&&&&&&&&&WNUMBER&&&&&&&&&&&&&&&&&&&&&&", strconv.Itoa(worker.Number), "RunMaster()", "master.go")
			mr.Workers[worker.Number] = worker
			workers = append(workers, newWorker)
			//go mr.AssignJobToIdleWorker("Map", i, newWorker.Worker, mr.nReduce, mapSyncChannel, workerFailureChannel)

		default:
			myLogger("-", "no registration", "RunMaster()", "master.go")
		}

		if IsIdleWorker() {
			worker := getAnIdleWorker()
			go mr.AssignJobToIdleWorker("Map", i, worker.Address, mr.nReduce, mapSyncChannel, workerFailureChannel)
			myLogger("iiiiiiiiiii", "I AM IDLE ", "RunMaster()", "master.go")
		} else {
			myLogger("nininiiniinininininin", "I AM NOT IDLE ", "RunMaster()", "master.go")
		}

		select {
		case mapJobDone := <-mr.MapJobChannel:
			worker := &WorkerInfo{}
			worker.Address = mapJobDone.Worker
			worker.Number = mapJobDone.JobNumber
			worker.IsIdle = true
			mr.Workers[worker.Number] = worker
			//go mr.AssignJobToIdleWorker("Map", i, worker.Address, mr.nReduce, mapSyncChannel, workerFailureChannel)
		}
	}

	<-mr.MapJobChannel

	for i := 0; i < len(workers); i++ {
		workers[i].isIdle = true
	}

	for i := 0; i < mr.nReduce; i++ {

		select {
		case worker := <-mr.registerChannel:
			workers = append(workers, worker)
			go mr.AssignJobToIdleWorker("Reduce", i, worker.Worker, mr.nMap, reduceSyncChannel, workerFailureChannel)

		default:
			index, workerName, isAvailibleWorker := getIdleWorker(workers)
			if isAvailibleWorker {
				workers[index].isIdle = false
				go mr.AssignJobToIdleWorker("Reduce", i, workerName, mr.nMap, reduceSyncChannel, workerFailureChannel)
			} else {
				reduceJobDone := <-mr.ReduceJobChannel
				go mr.AssignJobToIdleWorker("Reduce", i, reduceJobDone, mr.nMap, reduceSyncChannel, workerFailureChannel)
			}
		}
	}
	<-mr.ReduceJobChannel

	mr.Merge()
	return mr.KillWorkers()
}

// how to handle worker failures?
// a worker is considered failed if an RPC call fails.
// if a call fails then we must reschedule the job that we told that worker to do
// and then remove the worker from the pool of availible workers
// then we should continue the normal scheduling routine.

// assigning a job to a worker (the RPC call happens in inside a seperate thread of execution).
// therefore the the failure is first noticed in this seperate thread of execution. the notice must be sent
// to the masters main thread vai a channel.
//
//
