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
	mapSyncChannel := make(chan string, mr.nMap)
	reduceSyncChannel := make(chan string, mr.nReduce)
	workers := mr.RecieveWorkers()
	mr.DistributedMap(workers, mapSyncChannel)
	mr.MakeAllWorkersIdle(workers)
	mr.DistributedReduce(workers, reduceSyncChannel)
	mr.Merge()
	return mr.KillWorkers()
}
