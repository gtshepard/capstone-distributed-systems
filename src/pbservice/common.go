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
	Err   Err
	Value string // For PutHash
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

type SendCopyArgs struct{}
type SendCopyReply struct {
	Value bool
}

type ClientMsg struct {
	Key   string
	Value string
	Gid   int64
}

type Update struct {
	Key   string
	Value string
	Gid   int64
}

type DBCopy struct {
	Db  map[string]string
	Gid int64
}

type ReplicateArgs struct {
	Db  map[string]string
	Gid int64
}

type ReplicateReply struct {
	Err Err
	Db  map[string]string
	Gid int64
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
