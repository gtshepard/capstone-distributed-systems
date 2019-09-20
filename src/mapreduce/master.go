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

func (mr *MapReduce) ScheduleJob(nMap int, nReduce int, jobType JobType, done chan string, buffer chan string, workers []*RegisterArgs) {
	for i := 0; i < nMap; i++ {
		index, workerName, isAvailibleWorker := mr.getIdleWorker(workers)
		if isAvailibleWorker {
			workers[index].isIdle = false
			go mr.AssignJobToIdleWorker(jobType, i, workerName, nReduce, buffer)
		} else {
			idleWorker := <-done
			go mr.AssignJobToIdleWorker(jobType, i, idleWorker, nReduce, buffer)
		}
	}
	<-done
}

func (mr *MapReduce) RunMaster() *list.List {

	var workers []*RegisterArgs

	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		worker.isIdle = true
		workers = append(workers, worker)
		info := &WorkerInfo{}
		info.address = worker.Worker
		mr.Workers[worker.Worker] = info
	}

	//find an idle worker
	getIdleWorker := func(s []*RegisterArgs) (int, string, bool) {
		for i := 0; i < len(s); i++ {
			if s[i].isIdle {
				return i, s[i].Worker, true
			}
		}
		return 0, "", false
	}

	mapSyncChannel := make(chan string, mr.nMap)
	reduceSyncChannel := make(chan string, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		index, workerName, isAvailibleWorker := getIdleWorker(workers)
		if isAvailibleWorker {
			workers[index].isIdle = false
			go mr.AssignJobToIdleWorker("Map", i, workerName, mr.nReduce, mapSyncChannel)
		} else {
			mapJobDone := <-mr.MapJobChannel
			go mr.AssignJobToIdleWorker("Map", i, mapJobDone, mr.nReduce, mapSyncChannel)
		}
	}
	<-mr.MapJobChannel

	for i := 0; i < len(workers); i++ {
		workers[i].isIdle = true
	}

	for i := 0; i < mr.nReduce; i++ {
		index, workerName, isAvailibleWorker := getIdleWorker(workers)
		if isAvailibleWorker {
			workers[index].isIdle = false
			go mr.AssignJobToIdleWorker("Reduce", i, workerName, mr.nMap, reduceSyncChannel)
		} else {
			reduceJobDone := <-mr.ReduceJobChannel
			go mr.AssignJobToIdleWorker("Reduce", i, reduceJobDone, mr.nMap, reduceSyncChannel)
		}
	}
	<-mr.ReduceJobChannel

	mr.Merge()
	return mr.KillWorkers()
}
