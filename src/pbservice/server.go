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
	writer    chan *ClientMsg
	reader    chan *ClientMsg
	db        map[string]string
	update    chan *Update
	repilcate chan *DBCopy
	ack       chan *SrvAckArgs
	intervals int
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	msg := &ClientMsg{}
	msg.Key = args.Key
	msg.Value = args.Value
	pb.writer <- msg
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//make request to db

	msg := &ClientMsg{}
	msg.Key = args.Key

	pb.reader <- msg

	//get reply
	rep := <-pb.reader

	reply.Value = rep.Value

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
	// Your code here.
	myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK ", "", "$$$$$$$$$$$$$$")
	pb.intervals += 1
	myLogger("INTERVAL COUNT: ", strconv.Itoa(pb.intervals)+" SRV: "+pb.me, "Tick", "Server.go")
	if pb.dead {
		myLogger("DEAD FLAG", "SRV FAILED: "+pb.me, "Tick", "Server.go")
		time.Sleep(viewservice.PingInterval * 5)
	}

	view, _ := pb.vs.Get()

	if view.Viewnum < 2 {
		view, _ = pb.vs.Ping(0)
	} else {
		view, _ = pb.vs.Ping(view.Viewnum)
	}

	if pb.me == view.Primary {

		select {
		case read := <-pb.reader:
			myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK READ", "", "$$$$$$$$$$$$$$")
			msg := &ClientMsg{}
			if val, ok := pb.db[read.Key]; ok {
				//read given key from db
				msg.Key = read.Key
				msg.Value = val
				pb.reader <- msg
			} else {
				//attempted to read key that does not exist
				msg.Key = read.Key
				msg.Value = ""
				pb.reader <- msg
			}

		case write := <-pb.writer:
			pb.db[write.Key] = write.Value
			args := &PutArgs{}
			var reply *PutReply
			args.Key = write.Key
			args.Value = write.Value
			myLogger("After Recieve", pb.db[write.Key], "Tick", "Server.go")
			if ok := call(view.Backup, "PBServer.RecieveUpdate", args, &reply); !ok {
				myLogger("ReciveUpdate", "RPC FAIL", "Tick", "Server.go")
				return
			}
			// wait for update to be written to backup
			//<-pb.ack
		}

	} else if pb.me == view.Backup {

		select {
		case replica := <-pb.repilcate:
			pb.db = replica.Db
			myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK - BACKUP REPLICATE ", "", "$$$$$$$$$$$$$$")
			//call(view.Primary, "PBServer.Ack", args, &reply)
		case update := <-pb.update:
			myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK - BACKUP UPDATE ", "", "$$$$$$$$$$$$$$")
			pb.db[update.Key] = update.Value
			//call(view.Primary, "PBServer.Ack", args, &reply)
		default:
			myLogger("$$$$$$$$$$$$$$$", "PBSERVICE TICK - BACKUP DEFAULT ", "", "$$$$$$$$$$$$$$")
		}
	} else {

	}
}

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
	pb.writer = make(chan *ClientMsg)
	pb.reader = make(chan *ClientMsg)
	pb.update = make(chan *Update)
	pb.repilcate = make(chan *DBCopy)
	pb.db = make(map[string]string)
	pb.intervals = 0

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			//srv is alive
		}
		myLogger("~~~~~~~~~~~~~~~~~~~~", "CALL TO FAILURE MADE", "KV_SRV", "~~~~~~~~~~~~~~~~~~~")
		pb.vs.Failure(pb.me)
		pb.done.Done()
	}()

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
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
