package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}

	// Your declarations here.
	writer            chan *ClientMsg
	reader            chan *ClientMsg
	db                map[string]string
	update            chan *Update
	repilcate         chan *DBCopy
	ack               chan *SrvAckArgs
	intervals         int
	completedRequests map[int64]int64
	restart           bool
	copier            chan bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	msg := &ClientMsg{}
	msg.Key = args.Key
	msg.Value = args.Value
	msg.Gid = args.Gid
	pb.writer <- msg
	myLogger("^^^^^^^^^^^^^^^^^^^", "PUT MESSAGE: ", args.Value+pb.me, "^^^^^^^^^^^^^^^^^^^^^^")
	serverResponse := <-pb.writer
	myLogger("^^^^^^^^^^^^^^^^^^^", "RESPONSE", serverResponse.Value+pb.me, "^^^^^^^^^^^^^^^^^^^^^^")
	reply.Value = serverResponse.Value
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//make request to db
	msg := &ClientMsg{}
	msg.Key = args.Key
	msg.Gid = args.Gid
	pb.reader <- msg

	//get reply
	rep := <-pb.reader
	reply.Value = rep.Value

	return nil
}

func (pb *PBServer) SendCopy(args *SendCopyArgs, reply *SendCopyReply) error {
	if len(pb.db) == 0 {
		reply.Value = true
	} else {
		reply.Value = false
	}
	return nil
}

func (pb *PBServer) RecieveUpdate(args *PutArgs, reply *PutReply) error {
	update := &Update{}
	update.Key = args.Key
	update.Value = args.Value

	pb.update <- update
	return nil
}

func (pb *PBServer) RecieveReplica(args *ReplicateArgs, reply *ReplicateReply) error {
	replica := &DBCopy{}
	replica.Db = args.Db
	pb.repilcate <- replica
	return nil
}

func (pb *PBServer) Ack(args *SrvAckArgs, reply *SrvAckReply) error {
	pb.ack <- args
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {

	view, _ := pb.vs.Get()
	myLogger("&&&&&&&&&&&&&&&", "KNOWS VIEW "+strconv.Itoa(int(view.Viewnum))+": ", pb.me, "&&&&&&&&&&&&&")
	myLogger("&&&&&&&&&&&&&&&", "VIEW.PRIMARY  "+": ", view.Primary, "&&&&&&&&&&&&&")
	myLogger("&&&&&&&&&&&&&&&", "VIEW.BACKUP "+": ", view.Backup, "&&&&&&&&&&&&&")

	if pb.restart {
		myLogger("&&&&&&&&&&&&&&&", "START:", pb.me, "&&&&&&&&&&&&&")
		pb.restart = false
		view, _ = pb.vs.Ping(0)
	} else {
		myLogger("&&&&&&&&&&&&&&&", "PINGING VS FROM", pb.me, "&&&&&&&&&&&&&")
		view, _ = pb.vs.Ping(view.Viewnum)
	}

	if pb.me == view.Primary {

		if view.Backup != "" {
			view, _ = pb.vs.Ping(view.Viewnum)
			args := &SendCopyArgs{}
			var reply *SendCopyReply

			ok := call(view.Backup, "PBServer.SendCopy", args, &reply)
			for !ok {
				view, _ := pb.vs.Get()
				ok = call(view.Backup, "PBServer.SendCopy", args, &reply)
				myLogger("*******************", "RPC FAIL", "SEND COPY FROM"+pb.me, "****************")
			}

			if reply.Value {
				args := &ReplicateArgs{}
				var reply *ReplicateReply
				args.Db = pb.db

				ok := call(view.Backup, "PBServer.RecieveReplica", args, &reply)
				for !ok {
					view, _ := pb.vs.Get()
					myLogger("ReciveReplica", "RPC FAIL", "Tick", "Server.go")
					ok = call(view.Backup, "PBServer.RecieveReplica", args, &reply)
				}
			}
		}

		select {

		case read := <-pb.reader:
			msg := &ClientMsg{}
			if val, ok := pb.completedRequests[read.Gid]; !ok {
				pb.completedRequests[read.Gid] = read.Gid
				if val, ok := pb.db[read.Key]; ok {
					msg.Key = read.Key
					msg.Value = val
					pb.reader <- msg
				} else {
					msg.Key = read.Key
					msg.Value = ""
					pb.reader <- msg
				}
			} else {
				myLog("duplicate GET", val)
			}

		case write := <-pb.writer:

			serverResponse := &ClientMsg{}
			serverResponse.Gid = write.Gid
			serverResponse.Key = write.Key
			serverResponse.Value = write.Value

			if val, ok := pb.completedRequests[write.Gid]; !ok {
				pb.completedRequests[write.Gid] = write.Gid
				pb.db[write.Key] = write.Value
				args := &PutArgs{}
				var reply *PutReply
				args.Key = write.Key
				args.Value = write.Value
				args.Gid = write.Gid

				pb.writer <- serverResponse

				if view.Backup != "" {
					view, _ = pb.vs.Ping(view.Viewnum)
					ok := call(view.Backup, "PBServer.RecieveUpdate", args, &reply)
					for !ok {
						view, _ := pb.vs.Get()
						ok = call(view.Backup, "PBServer.RecieveUpdate", args, &reply)
						myLogger("*******************", "RPC FAIL", "SEND UPDATE TO BACKUP", "****************")
					}
				}

			} else {
				myLog("duplicate PUT", val)
			}
		default:
			myLogger("$^$^$^$^$^$^$^$$^$^$", "PRIMARY TICKING: ", pb.me, "$^$^$^$^$^$^$^$$^$^$")
		}

	} else if pb.me == view.Backup {

		select {
		case replica := <-pb.repilcate:
			if val, ok := pb.completedRequests[replica.Gid]; !ok {
				pb.db = replica.Db
				myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK - BACKUP REPLICATE ", "", "$$$$$$$$$$$$$$")
			} else {
				myLog("DUPLICATE REPLICA", val)
				myLogger("$$$$$$$$$$$$$$$", " DUPLICATE - BACKUP REPLICATE ", "", "$$$$$$$$$$$$$$")
			}

		case update := <-pb.update:
			if val, ok := pb.completedRequests[update.Gid]; !ok {
				myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK - BACKUP UPDATE ", "", "$$$$$$$$$$$$$$")
				pb.db[update.Key] = update.Value
			} else {
				myLog("DUPLICATE UPDATE", val)
				myLogger("$$$$$$$$$$$$$$$", "DUPLICATE - BACKUP UPDATE ", "", "$$$$$$$$$$$$$$")
			}
		default:
			//	myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK - BACKUP DEFAULT ", "", "$$$$$$$$$$$$$$")
		}
	} else {
		myLogger("$$$$$$$$$$$$$$$", "IDLE: "+pb.me, "", "$$$$$$$$$$$$$$")
	}
}

//fails to ACK on backu up up election. no pings are sent

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.copier = make(chan bool)
	pb.writer = make(chan *ClientMsg)
	pb.reader = make(chan *ClientMsg)
	pb.update = make(chan *Update)
	pb.repilcate = make(chan *DBCopy)
	pb.db = make(map[string]string)
	pb.completedRequests = make(map[int64]int64)
	pb.intervals = 0
	pb.restart = true

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			myLogger("$^$^$^$^$^$^$^$$^$^$", "TICK--TICK--TICK: ", pb.me, "$^$^$^$^$^$^$^$$^$^$")
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}

// System Assumptions: Fault tolerant key/value service
//  - K/V service should with stand up to n-1 faults (excluding view service)
//  - use primary/backup scheme with view service
//	- system not exepcted to work correctly if view service goes down (SPOF)
//
//
//
//
