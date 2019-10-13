package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
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
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
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

// PROTOCOL DESIGN
//
//client.go
//PutExt(k, v, h):
//
// need a client to repeatedly send requests until it recieves an ACK from the primary that
// signals a single request has completed.
//
// before a client sends a sequence of requests a unique group_id must be generated.
// a group_id is a random number with a high probability of being unique.
// a request is an RPC call to either Server.Get or Server.Put
//
// a client that sends requests until the client learns one is successful from the server
// Get(k):
//   group_id = GetUniqueNumber()
//	 v = vs.clerk.get()
//   ack = &Ack{}
//   isDone = false
//	 group_id = GetRandomUniqueNumber()
//
//   while !isDone:
//		arg.group_id = group_id
//		arg.key = k
//		if v.p == "":
//			return
//
//		select:
//			//client recieves a broadacasted ack,
//			case r_ack = <- broadcast_ack:
//			   ack.group_id = r_ack.group_id
//             // check to see if ack is for this client
//			   if group_id == ack.group_id:
//					isDone = true
//					break
//			   else:
//					//if no successful request, send another
//					call(v.p, "Server.Get", GetArg, GetReply)
//			 default:
//					//if no successful request, send another
//		  			call(v.p, "Server.Get", GetArg, GetReply)
//
//
// PutExt(k , v , h):
//	 group_id = GetUniqueNumber()
//	 v = vs.clerk.get()
//   ack = &Ack{}
//   ack.is_done = false
//	 ack.group_id = -1
//
//   while !isDone:
//		arg.group_id = group_id
//		arg.key = k
//
//		if v.p == "":
//			return
//
//		v = vs.clerk.get()
//
//		select:
//			//client recieves a broadacasted ack,
//			case r_ack = <- broadcast_ack:
//			   ack.group_id = r_ack.group_id
//             // check to see if ack is for this client
//			   if group_id == ack.group_id:
//					isDone = true
//					break
//
//			 default:
//					//if no successful request, send another
//				if h:
//					call(v.p, "Server.Get", GetArgs, GetReply)
//					exists := <- check
//					//hash value
//					if exists:
//						//key already exists
//						previous_val = GetArgs.val
//						value_to_store = hash(previous_val + v)
//		    		else:
//						value_to_store = hash("" + v)
//						//send key/hashed value to db
//						PutArgs.key = k
//						PutArgs.val = v
//						call(v.p, "Server.Put",PutArgs, PutReply)
// 			   else:
//					//send key/value to db
//					PutArgs.key = k
//					PutArgs.val = v
// 					call(v.p, "Server.Put",PutArgs, PutReply)
//
// RecieveBroadcastedAck(args, reply):
//      broadcast_ack <- args
//
// server.go
//  if the server is primary it should handle requests by clients, otherwise it should not.
//  a server knows whether or not it is primary by contacting the view service.
//	each request (Get or Put) is handled on a seperate thread.
//
// Get(args, reply):
//	  v = vs.clerk.get()
//	  if pb.me == v.p:
//		 vs.reads <- args
//		 <-read.resp
//	  else:
//		 print("not primary,request not accepted")
//
// Put(args, reply):
//	  v = vs.clerk.get()
//	  if pb.me == v.p:
//		vs.writes <- args
//		<- write.resp
//    else:
//       print("not primary or backup, idle servers do nothing")
//
//RecieveCopy(args, reply):
//		vs.copy <- args
//
//RecieveUpdate(args, reply):
//		vs.update <- args
//
// Tick():
//	 //determine server role
//	 //set just promoted flag in event server was just promoted
//	 //how does one know sevrer was promoted
//
//   //only primary handles incoming requests
//	if pb.me == v.p && JustPromoted:
//		CopyArg.db = map
//		call(v.b,"Server.RecieveCopy", CopyArg, CopyReply)
//		<-WaitForBackUpAck
//
//	if pb.me == v.p:
//		select:
//			case read := <- reads:
//				if !success.contains(read.key):
//					if db.contains(read.group_id):
//						read.resp <- map[read.key]
//						success[read.group_id] = read.group_id
//						call(client, "Client.ReicveBroadcastedAck", AckArgs, AckReply)
//					else:
//						print("key doesnt exist in db")
//						//how do we notify client in this event to stop making gets?
//				else:
//					print("duplicate read request ignored: ", group_id)
//
//			case write := <- writes:
//			if !success.contains(write.group_id):
//				//write to db
//				db[write.key] = write.val
//				//send update to back up
//				call(v.b, "Server.RecieveUpdate",PutArgs, PutReply)
//				<-WaitForBackUpAck
//				//write complete
//				write.resp <- true
//				success[write.group_id] = write.group_id
//				call(client, "Client.ReicveBroadcastedAck", AckArgs, AckReply)
//			else:
//
//				print("duplicate write request ignored: ", group_id)
//
//	 //backup recieves updates from primary and no other
//   else if pb.me 	== v.b:
//		select:
//			case entry <- vs.update:
//				db[entry.key] = entry.val
//				call(v.p, "Server.Ack", AckArg, AckReply)
//			case db_data <- vs.copy:
//				for k,v in db_data:
//					db[k] = v
//				//ack copy done for backup
//				call(v.p, "Server.Ack", AckArg, AckReply)
//	else:
//		print("idle server")
//
// Ack(AckArg, AckReply):
//	 vs.waitForBackUp <- AckArg
//
