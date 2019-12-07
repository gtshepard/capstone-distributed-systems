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
	// You'll have to add definitions here.

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
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ClientMsg struct {
	Key   string
	Value string
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

func myLog(id int64) {
	f, err := os.OpenFile("testID", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("group_id:", id)
}

func groupIdForRequest() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
