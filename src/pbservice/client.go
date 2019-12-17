package pbservice

import (
	"fmt"
	"net/rpc"
	"strconv"
	"viewservice"
)

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Please Wait...")
	return false
}

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().

func (ck *Clerk) Get(key string) string {
	gid := groupIdForRequest()
	args := &GetArgs{}
	args.Key = key
	var reply *GetReply
	var baseReply GetReply
	baseReply.Value = ""
	reply = &baseReply
	args.Gid = gid
	view, _ := ck.vs.Get()

	ok := call(view.Primary, "PBServer.Get", args, &reply)
	for !ok {
		view, _ := ck.vs.Get()
		//myLogger("@@@@@@@@@@@@@@@@@@@@@@@@@@@@", "GET RPC CALL FAILED....RETRYING", "", "@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		ok = call(view.Primary, "PBServer.Get", args, &reply)
	}
	for view.Primary == "" {
	}

	return reply.Value
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

	gid := groupIdForRequest()
	view, _ := ck.vs.Get()
	putArgs := &PutArgs{}
	putArgs.Gid = gid

	var putReply *PutReply
	var basePutReply PutReply
	putReply = &basePutReply

	if dohash {
		prev := ck.Get(key)
		hashedValue := strconv.Itoa(int(hash(prev + value)))
		putArgs.Key = key
		putArgs.Value = hashedValue
		myLogger("@@@@@@@@@@@@@@@@@@@@@@@@@@@@", "key: "+key, "value:"+value, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		myLogger("@@@@@@@@@@@@@@@@@@@@@@@@@@@@", "prev:"+prev, "hashed-value:"+hashedValue, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		ok := call(view.Primary, "PBServer.Put", putArgs, putReply)
		for !ok {
			view, _ := ck.vs.Get()
			myLogger("@@@@@@@@@@@@@@@@@@@@@@@@@@@@", "PUT HASH RPC CALL FAILED....RETRYING", "", "@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
			ok = call(view.Primary, "PBServer.Put", putArgs, putReply)
		}
		myLogger("@@@@@@@@@@@@@@@@@@@@@@@@@@@@", "RPC RECIEVED A REPLY", "", "@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		return prev
	} else {
		// put value not hash
		putArgs.Key = key
		putArgs.Value = value
		ok := call(view.Primary, "PBServer.Put", putArgs, &putReply)
		for !ok {
			view, _ := ck.vs.Get()
			myLogger("@@@@@@@@@@@@@@@@@@@@@@@@@@@@", "PUT HASH RPC CALL FAILED....RETRYING", "", "@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
			ok = call(view.Primary, "PBServer.Put", putArgs, &putReply)
		}
		return key
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
