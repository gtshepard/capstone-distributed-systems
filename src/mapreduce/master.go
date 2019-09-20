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

func (mr *MapReduce) AssignJobToIdleWorker(job JobType, jobNumber int, worker string, otherPhase int, c chan string) {
	myLogger("RM-8", "Schedule Job Start", "RunMaster()", "master.go")

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

	myLogger("RM-11", "Schedule Job End"+strconv.Itoa(jobNumber), "RunMaster()", "master.go")
	c <- worker
}

func (mr *MapReduce) RunMaster() *list.List {
	//register workers and set them to idle
	myLogger("RM-1", "START OF TEST", "RunMaster()", "master.go")
	var workers []*RegisterArgs

	for i := 0; i < 2; i++ {
		worker := <-mr.registerChannel
		worker.isIdle = true
		myLogger("RM-2", "register worker: "+worker.Worker, "RunMaster() - register-worker", "master.go")
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
		//schedule worker
		if isAvailibleWorker {
			workers[index].isIdle = false
			//schedule Reduce Jobs concurrently
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
		//schedule worker
		if isAvailibleWorker {
			workers[index].isIdle = false
			//schedule Reduce Jobs concurrently
			go mr.AssignJobToIdleWorker("Reduce", i, workerName, mr.nMap, reduceSyncChannel)

		} else {
			myLogger("RM-2", "not idle ", "RunMaster() - register-worker", "master.go")
			reduceJobDone := <-mr.ReduceJobChannel
			myLogger("RM-2", "not idle recieve job  "+reduceJobDone, "RunMaster() - register-worker", "master.go")
			//completedReduceJobs = append(completedMapJobs, reduceJobDone)
			go mr.AssignJobToIdleWorker("Reduce", i, reduceJobDone, mr.nMap, reduceSyncChannel)
		}
		myLogger("&&&&&&&&&&", "Reduce channel item count"+strconv.Itoa(len(reduceSyncChannel)), "RunMaster() - register-worker", "master.go")
	}
	<-mr.ReduceJobChannel

	mr.Merge()
	return mr.KillWorkers()

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

}
