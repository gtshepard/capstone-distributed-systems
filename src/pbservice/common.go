package pbservice

import (
	"crypto/rand"
	"hash/fnv"
	"log"
	"math/big"
	"os"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	Gid    int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
	Error         string
}

type GetArgs struct {
	Key string
	Gid int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ClientMsg struct {
	Key   string
	Value string
	Gid   int64
}

type Update struct {
	Key   string
	Value string
}

type DBCopy struct {
	Db map[string]string
}

type ReplicateArgs struct {
	Db map[string]string
}

type ReplicateReply struct {
	Err Err
	Db  map[string]string
}

type SrvAckArgs struct{}
type SrvAckReply struct{}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func myLogger(step string, msg string, call string, file string) {
	f, err := os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println(step, ": ", msg, "-", call, "-", file)
}

func myLog(process string, reqGid int64) {
	f, err := os.OpenFile("testID", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println(process, ":", "gid:", reqGid)
}

func groupIdForRequest() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// request are sent by the client until, a request is successful
// implementing at most once semantics
// unique group_id for a set of requests sent to the primary
// if a request is processed, no further incoming requests with same group_id should be processed
// to filter out duplicate requests that could arrive after a succesful request has already been made
// (the peroid in between the acknowldgement and the successful processing of the request)
// attach a group_id to a client request. any requests sent within one call to get has the same group id
// each time the primary server handles an incoming request it should check to see if a request
// with an exisiting group_id has been made, if so, the request is ingored. if not, the request is processed
// when a request has successfully been processed the group_id should be recorded in a map
// the actual request data need not be saved.

//completed_requests[int64]int64
