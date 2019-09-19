package mapreduce

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)

const (
	Map    = "Map"
	Reduce = "Reduce"
)

type JobType string

// RPC arguments and replies.  Field names must start with capital letters,
// otherwise RPC will break.

type DoJobArgs struct {
	File          string
	Operation     JobType
	JobNumber     int // this job's number
	NumOtherPhase int // total number of jobs in other phase (map or reduce)
	Master        string
	Worker        string
}

type DoJobReply struct {
	OK bool
}

type ShutdownArgs struct {
}

type ShutdownReply struct {
	Njobs int
	OK    bool
}

type RegisterArgs struct {
	Worker string
}

type RegisterReply struct {
	OK bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// call() returns true if the server responded, and false
// if call() was not able to contact the server. in particular,
// reply's contents are valid if and only if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in master.go, mapreduce.go,
// and worker.go.  please don't change this function.
//

// the callee of this function is considered the client
// srv string: the entity you want to contact (in our case master or worker) which is considered the server
// rpcname string:  name is the procedure you want to call
// args interface{}: accepts the data you want to send over
// reply interface{}: place holder for the reply (usaully passed by reference)
// bool: returns true if client successfully made sent request to server, false if not
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {

	// contact server, if successful return a client
	// client/server will communicate via unix domain sockets
	// (communication between local processes the client & server)
	c, errx := rpc.Dial("unix", srv)

	//request to connect fails
	if errx != nil {
		return false
	}

	//defers the close of the connection c until the call function returns
	defer c.Close()

	//client calls the method on the server passing the data needed and a placeholder for the reply
	err := c.Call(rpcname, args, reply)
	//if success
	if err == nil {
		return true
	}
	//if call fails
	fmt.Println(err)
	return false
}

//creates or opens log file then appends message to log
func myLogger(step string, msg string, call string, file string) {
	f, err := os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println(step, ": ", msg, "-", call, "-", file)
}
