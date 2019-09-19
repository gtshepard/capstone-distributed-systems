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

func (mr *MapReduce) AssignJobToIdleWorker(job JobType, jobNumber int, worker string, otherPhase int) {
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
	myLogger("RM-11", "Schedule Job End", "RunMaster()", "master.go")
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
		//i need to schedule workers to do map jobs on seperate threads
	}

	//find an idle worker
	getIdleWorker := func(s []*RegisterArgs) (int, string, bool) {
		for i := 0; i < len(s); i++ {
			if s[i].isIdle {
				//myLogger("RM-3", "Found Idle Worker", "getIdleWorker", "master.go")
				return i, s[i].Worker, true
			} else {
				//	myLogger("RM-3", "No Idle Worker", "RunMaster()", "master.go")
			}
		}
		return 0, "", false
	}

	completedMapJobs := make([]string, 0)
	mapSyncChannel := make(chan string)

	//var mapIdleWg sync.WaitGroup
	//var mapNonIdleWg sync.WaitGroup
	//schedule all M map jobs
	//jobs can only be scheduled on idle workers
	for i := 0; i < mr.nMap; i++ {
		index, workerName, isAvailibleWorker := getIdleWorker(workers)

		if isAvailibleWorker {
			//myLogger("$$$$$$$$$$$$$$$$$$$$$$$$$$$", "isAvailebleWorker: "+strconv.FormatBool(isAvailibleWorker), "RunMaster()", "master.go")
			workers[index].isIdle = false
			go func(worker string, job int, c chan string) {
				//myLogger("RM-5", "start go routine to schedule", "RunMaster()", "master.go")
				c <- worker
				mr.AssignJobToIdleWorker("Map", i, worker, mr.nReduce)
				<-mr.MapJobChannel
			}(workerName, i, mapSyncChannel)

		} else {
			//myLogger("RM-5", "Wait for Worker - isAvailibleWorker: "+strconv.FormatBool(isAvailibleWorker), "RunMaster()", "master.go")
			//	mapJobDone := <-mr.MapJobChannel
			//	completedMapJobs = append(completedMapJobs, mapJobDone)
			//nowAvailbleWorker := mapJobDone
			//	myLogger("RM-6", "Schedule Newly Availible Worker ", "RunMaster()", "master.go")
			newlyAvailibleWorker := <-mapSyncChannel

			go func(worker string, job int, c chan string) {
				//	myLogger("RM-6", "start go routine to schedule Map", "RunMaster()", "master.go")
				c <- worker
				mr.AssignJobToIdleWorker("Map", i, worker, mr.nReduce)
				<-mr.MapJobChannel
			}(newlyAvailibleWorker, i, mapSyncChannel)

			//myLogger("RM-6", "Map Done By Worker: "+mapJobDone, "RunMaster()", "master.go")
		}
	}

	myLogger("RM-13", "Number of Map Jobs Done: "+strconv.Itoa(len(completedMapJobs)), "RunMaster()", "master.go")
	fmt.Println("Number of Map Jobs Done: " + strconv.Itoa(len(completedMapJobs)))

	//finish remaining map jobs
	i := 0
	for len(completedMapJobs) != mr.nMap {
		mapJobDone := <-mr.MapJobChannel
		//mapWg.Wait()
		myLogger("RM----", strconv.Itoa(i)+"-"+mapJobDone, "RunMaster()", "master.go")
		completedMapJobs = append(completedMapJobs, mapJobDone)
		myLogger("RM-14", "Number of Map Jobs Done: "+strconv.Itoa(len(completedMapJobs)), "RunMaster()", "master.go")
		fmt.Println("Number of Map Jobs Done: " + strconv.Itoa(len(completedMapJobs)))
	}

	//all jobs completed, set all workers to idle
	for _, w := range workers {
		w.isIdle = true
	}

	// completedReduceJobs := make([]string, 0)
	// //schedule all R Reduce jobs
	// myLogger("RM-15", "REDUCE ABOUT TO RUN ", "RunMaster()", "master.go")
	// for i := 0; i < mr.nReduce; i++ {
	// 	index, workerName, isAvailibleWorker := getIdleWorker(workers)

	// 	//schedule worker
	// 	if isAvailibleWorker {
	// 		workers[index].isIdle = false
	// 		//schedule Reduce Jobs concurrently
	// 		go func(worker string, job int) {
	// 			myLogger("RM-12", "start go routine to schedule Reduce", "RunMaster()", "master.go")
	// 			mr.ScheduleJob("Reduce", job, worker, mr.nMap)
	// 		}(workerName, i)

	// 	} else {

	// 		myLogger("RM-12", "Wait for Reduce Worker - isAvailibleWorker: "+strconv.FormatBool(isAvailibleWorker), "RunMaster()", "master.go")
	// 		reduceJobDone := <-mr.ReduceJobChannel
	// 		completedReduceJobs = append(completedReduceJobs, reduceJobDone)
	// 		nowAvailbleWorker := reduceJobDone

	// 		myLogger("RM-13", "Schedule Newly Availible Worker ", "RunMaster()", "master.go")
	// 		go func(worker string, job int) {
	// 			mr.ScheduleJob("Reduce", job, worker, mr.nMap)
	// 		}(nowAvailbleWorker, i)

	// 		myLogger("RM-14", "Redudce Done By Worker: "+reduceJobDone, "RunMaster()", "master.go")
	// 	}
	// }

	// fmt.Println("Number of Reduce Jobs Done: " + strconv.Itoa(len(completedReduceJobs)))

	// for mr.nReduce != len(completedReduceJobs) {
	// 	reduceJobDone := <-mr.ReduceJobChannel
	// 	completedReduceJobs = append(completedReduceJobs, reduceJobDone)
	// 	fmt.Println("Number of Map Jobs Done: " + strconv.Itoa(len(completedMapJobs)))
	// }

	// for _, w := range workers {
	// 	w.isIdle = true
	// }
	// //schedule reduce jobs
	// for i := 0; i < mr.nReduce; i++ {
	// 	index, workerName, isAvailibleWorker := getIdleWorker(workers)

	// 	if isAvailibleWorker {
	// 		myLogger("RM-4", "isAvailebleWorker: "+strconv.FormatBool(isAvailibleWorker), "RunMaster()", "master.go")
	// 		workers[index].isIdle = false

	// 		go func(worker string, job int) {
	// 			myLogger("RM-5", "start go routine to schedule", "RunMaster()", "master.go")
	// 			mr.ScheduleJob("Reduce", i, worker, mr.nMap)
	// 		}(workerName, i)

	// 	} else {

	// 		myLogger("RM-5", "Wait for Worker - isAvailibleWorker: "+strconv.FormatBool(isAvailibleWorker), "RunMaster()", "master.go")
	// 		reduceJobDone := <-mr.ReduceJobChannel
	// 		completedReduceJobs = append(completedReduceJobs, reduceJobDone)
	// 		nowAvailbleWorker := reduceJobDone
	// 		myLogger("RM-6", "Schedule Newly Availible Worker ", "RunMaster()", "master.go")

	// 		go func(worker string, job int) {
	// 			myLogger("RM-6", "start go routine to schedule", "RunMaster()", "master.go")
	// 			mr.ScheduleJob("Reduce", i, worker, mr.nMap)
	// 		}(nowAvailbleWorker, i)

	// 		myLogger("RM-6", "Map Done By Worker: "+reduceJobDone, "RunMaster()", "master.go")
	// 	}
	// }

	// fmt.Println("Number of Reduce Jobs Done: " + strconv.Itoa(len(completedReduceJobs)))

	// for mr.nReduce != len(completedReduceJobs) {
	// 	reduceJobDone := <-mr.ReduceJobChannel
	// 	completedReduceJobs = append(completedReduceJobs, reduceJobDone)
	// 	fmt.Println("Number of Reduce Jobs Done: " + strconv.Itoa(len(completedReduceJobs)))
	// }

	myLogger("RM-12", "Scheduled All Map Jobs - Exit Map Loop", "RunMaster()", "master.go")
	myLogger("RM-13", "END OF TEST", "RunMaster()", "master.go")

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

	// we have T tasks that must be done by W workers. a worker can only do one task at time. W workers will be doing work concurrently
	// if no worker is idle, we must wait for a worker to become idle. a worker is idle when its task has been completed,
	// and the thread of execution the task was started on has finsihed running.
	// before we schedule a task on a worker, we must garuntee that our worker is indeed idle
	// for the first W tasks, we can schedule tasks on workers without waiting because all W workers
	// intially start out idle.
	// after the first W tasks have been scheduled, how do we wait for a worker W_i (where 0 <= i <= W) to become idle
	// and then schedule W_i along side the other W - 1 workers (workers work concurrently) ?
	// G = number of threads that make requests. there will be at most G threads. note G = W
	// to do this we must know when a task T_i (0<= i <= T) is completed and know when a thread G_i (0 <= i <= G)that we made the
	// request from is finished executing.
	//
	// since this routine is executing along side the masters thread of execution, we must pass a message from the thread we make a request on
	// to our master nodes thread of execution to notify master that the thread is done.
	// we must also send a request to the MapReduce program from the worker working on a  that the task has been completed.
	// once this message has been recieved  we must
	// we can do this as follows
	// note: a the scheudling go rotuine should never complete until the job has fininshed
	// for each Task:
	// 		if idleWorkerIsAvailible:
	//				go func():
	//					defer c <- true //send message to master signaling thread has stopped exeucting
	//					AssignJobToIdleWorker()
	//					<-WaitForJobToComplete //waits for a message to sognal job completiion, blocks until recieved
	// 		else: //no idle workers
	//				<- c // wait for messag
	//				go func():
	//					defer c <- true //send message to master signaling thread has stopped exeucting
	//					AssignJobToIdleWorker()
	//					<-WaitForJobToComplete//blocks until a message to has been recie
	//
	//

}
