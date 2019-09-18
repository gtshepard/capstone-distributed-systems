package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Worker is a server waiting for DoJob or Shutdown RPCs

type Worker struct {
	name   string
	Reduce func(string, *list.List) string
	Map    func(string) *list.List
	nRPC   int
	nJobs  int
	l      net.Listener
}

// The master sent us a job
//arg *DoJobArgs: info about the job
//res *DoJobReply: response after job started
func (wk *Worker) DoJob(arg *DoJobArgs, res *DoJobReply) error {
	fmt.Printf("Dojob %s job %d file %s operation %v N %d\n",
		wk.name, arg.JobNumber, arg.File, arg.Operation,
		arg.NumOtherPhase)
	switch arg.Operation {
	case Map:
		// do a map job based on user defined map function
		DoMap(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case Reduce:
		//do a reduce based on a user define reduce function
		DoReduce(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Reduce)
	}
	res.OK = true
	return nil
}

// The master is telling us to shutdown. Report the number of Jobs we
// have processed.
func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown %s\n", wk.name)
	res.Njobs = wk.nJobs
	res.OK = true
	wk.nRPC = 1 // OK, because the same thread reads nRPC
	wk.nJobs--  // Don't count the shutdown RPC
	return nil
}

// Tell the master we exist and ready to work
func Register(master string, me string) {
	//register data
	args := &RegisterArgs{}
	//workers address (refreed to as its name,)
	args.Worker = me
	var reply RegisterReply
	//make call as client to master server and use its regiester method as service
	//we send over worker address (name)
	//we get a bool reol but just store the value its not used
	ok := call(master, "MapReduce.Register", args, &reply)
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, me string,
	MapFunc func(string) *list.List,
	ReduceFunc func(string, *list.List) string, nRPC int) {
	DPrintf("RunWorker %s\n", me)
	//make new worker and give it the values it needs
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	//register wokrer as server (methods now availbel as as service )
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"

	//listen for requests on worker adress
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	//notify master of wokrer server
	Register(MasterAddress, me)

	// DON'T MODIFY CODE BELOW
	for wk.nRPC != 0 {
		//block until incoming request
		conn, err := wk.l.Accept()
		if err == nil {
			wk.nRPC -= 1
			//hsndle request as a seperate thread, blocks unitl client hangs up
			go rpcs.ServeConn(conn)
			//adjust job count
			wk.nJobs += 1
		} else {
			break
		}
	}
	wk.l.Close()
	DPrintf("RunWorker %s exit\n", me)
}
