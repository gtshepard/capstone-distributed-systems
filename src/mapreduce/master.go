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

func (mr *MapReduce) AssignJobToIdleWorker(job JobType, jobNumber int, worker string, otherPhase int, c chan string) {

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
		myLogger("RM-9", "Successful RPC call to worker", "RunMaster()", "master.go")
	} else {
		myLogger("RM-10", "RPC call to worker failed", "RunMaster()", "master.go")
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
			go mr.AssignJobToIdleWorker("Map", i, workerName, mr.nReduce, buffer)
		} else {
			idleWorker := <-mr.MapJobChannel
			go mr.AssignJobToIdleWorker("Map", i, idleWorker, mr.nReduce, buffer)
		}
	}
	<-mr.MapJobChannel
}

func (mr *MapReduce) DistributedReduce(workers []*RegisterArgs, buffer chan string) {
	for i := 0; i < mr.nReduce; i++ {
		index, workerName, isAvailibleWorker := mr.getIdleWorker(workers)
		if isAvailibleWorker {
			workers[index].isIdle = false
			go mr.AssignJobToIdleWorker("Reduce", i, workerName, mr.nMap, buffer)
		} else {
			idleWorker := <-mr.ReduceJobChannel
			go mr.AssignJobToIdleWorker("Reduce", i, idleWorker, mr.nMap, buffer)
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
		info.address = worker.Worker
		mr.Workers[worker.Worker] = info
	}
	return workers
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

	// for i := 0; i < 2; i++ {
	//  select {
	//  case worker := <-mr.registerChannel:
	//      worker.isIdle = true
	//      myLogger("RM-2", "register worker: "+worker.Worker, "RunMaster() - register-worker", "master.go")
	//      workers = append(workers, worker)
	//      info := &WorkerInfo{}
	//      info.address = worker.Worker
	//      mr.Workers[worker.Worker] = info
	//  default:
	//      myLogger("RM-2", "FAIL TO REGISTER", "RunMaster() - register-worker", "master.go")
	//  }
	// }

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

	completedMapJobs := make([]string, 0)
	completedReduceJobs := make([]string, 0)
	mapSyncChannel := make(chan string, mr.nMap)
	reduceSyncChannel := make(chan string, mr.nReduce)

	//var mapIdleWg sync.WaitGroup
	//var mapNonIdleWg sync.WaitGroup
	//schedule all M map jobs
	//jobs can only be scheduled on idle workers

	// select {
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
		case worker := <-mr.registerChannel:
			workers = append(workers, worker)
			go mr.AssignJobToIdleWorker("Map", i, worker.Worker, mr.nReduce, mapSyncChannel)
		default:
			mapJobDone := <-mr.MapJobChannel
			completedMapJobs = append(completedMapJobs, mapJobDone)
			go mr.AssignJobToIdleWorker("Map", i, mapJobDone, mr.nReduce, mapSyncChannel)
			myLogger("RM-2", "FAIL TO REGISTER", "RunMaster() - register-worker", "master.go")

		}
	}

	newlyAvailibleWorker := <-mr.MapJobChannel
	completedMapJobs = append(completedMapJobs, newlyAvailibleWorker)

	for i := 0; i < len(workers); i++ {
		workers[i].isIdle = true
	}

	for i := 0; i < mr.nReduce; i++ {

		select {
		case worker := <-mr.registerChannel:
			workers = append(workers, worker)
			go mr.AssignJobToIdleWorker("Reduce", i, worker.Worker, mr.nMap, reduceSyncChannel)

		default:
			index, workerName, isAvailibleWorker := getIdleWorker(workers)
			if isAvailibleWorker {
				workers[index].isIdle = false
				go mr.AssignJobToIdleWorker("Reduce", i, workerName, mr.nMap, reduceSyncChannel)
			} else {
				reduceJobDone := <-mr.ReduceJobChannel
				go mr.AssignJobToIdleWorker("Reduce", i, reduceJobDone, mr.nMap, reduceSyncChannel)
				myLogger("RM-2", "FAIL TO REGISTER", "RunMaster() - register-worker", "master.go")
			}
		}
	}

	Rna := <-mr.ReduceJobChannel
	completedReduceJobs = append(completedReduceJobs, Rna)

	mr.Merge()
	return mr.KillWorkers()

	mr.Merge()
	return mr.KillWorkers()
}

// how to handle worker failures?
// a worker is considered failed when an RPC call is made
// if a worker is considered failed if an RPC call fails
