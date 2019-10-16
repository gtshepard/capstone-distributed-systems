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

type SrvView struct {
	Viewnum uint
	Primary string
	Backup  string
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
	view      SrvView
	intervals int
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	//myLogger("RPC SUCCESS", "BEFORE SEND", "Tick", "Server.go")
	msg := &ClientMsg{}
	msg.Key = args.Key
	msg.Value = args.Value
	pb.writer <- msg
	//myLogger("RPC SUCCESS", "AFTER SEND", "Tick", "Server.go")
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	myLogger("GET", "BEFORE SEND", "GET", "Server.go")

	//make request to db
	msg := &ClientMsg{}
	msg.Key = args.Key
	pb.reader <- msg

	//get reply
	rep := <-pb.reader
	myLogger("GET", "AFTER VAL", "Tick", "Server.go")
	reply.Value = rep.Value
	myLogger("GET", "AFTER VAL", "Tick", "Server.go")
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
	replica.db = args.db
	pb.repilcate <- replica
	return nil
}

func (pb *PBServer) Ack(args *SrvAckArgs, reply *SrvAckReply) error {
	pb.ack <- args
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.intervals += 1
	myLogger("INTERVAL COUNT: ", strconv.Itoa(pb.intervals)+" SRV: "+pb.me, "Tick", "Server.go")
	if pb.dead {
		myLogger("DEAD FLAG", "SRV FAILED: "+pb.me, "Tick", "Server.go")
		time.Sleep(viewservice.PingInterval * 5)
	}
	//server learns its role from view service, p/b/i
	//handle request
	view, _ := pb.vs.Get()

	if view.Viewnum < 2 {
		view, _ = pb.vs.Ping(0)
	} else {
		view, _ = pb.vs.Ping(view.Viewnum)
	}

	//if view.Primary != "" || view.Backup {

	//} else {
	//	view, _ = pb.vs.Ping(0)
	//}

	//3 servers are made
	// server 1 is elected primary
	// server 2 is elected backup up
	//from then on they keep learning the newest view
	// server 3 is made but view is < 3, it wil just learn the newest view

	//handle put/get request for primary
	if pb.me == view.Primary {

		select {
		case read := <-pb.reader:

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
		//	select:
		//			case entry <- vs.update:
		//				db[entry.key] = entry.val
		//				call(v.p, "Server.Ack", AckArg, AckReply)
		//			case db_data <- vs.copy:
		//				for k,v in db_data:
		//					db[k] = v
		//				//ack copy done for backup
		//				call(v.p, "Server.Ack", AckArg, AckReply)

		//args := &SrvAckArgs{}
		//var reply *SrvAckReply

		select {
		case replica := <-pb.repilcate:
			pb.db = replica.db
			//call(view.Primary, "PBServer.Ack", args, &reply)
		case update := <-pb.update:
			pb.db[update.Key] = update.Value
			//call(view.Primary, "PBServer.Ack", args, &reply)
		}

	} else {

	}
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
	// Your pb.* initializations here.\
	pb.writer = make(chan *ClientMsg)
	pb.reader = make(chan *ClientMsg)
	pb.update = make(chan *Update)
	pb.repilcate = make(chan *DBCopy)
	pb.db = make(map[string]string)
	pb.intervals = 0
	//get a view for the srv
	// v, _ := pb.vs.Ping(0)
	// pb.view = SrvView{}
	// pb.view.Viewnum = v.Viewnum
	// pb.view.Primary = v.Primary
	// pb.view.Backup = v.Backup

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
// how does a server learn  its role
// if isIntialElection:
//		vs.clerk.ping(0)
//	else:
//		vs.clerk.ping(n)
//
//client.go
// PutExt(k, v, h):
//
// need a client to repeatedly send requests until it recieves an ACK from the primary that
// signals a single request has completed.
//
// before a client sends a sequence of requests a unique group_id must be generated.
// a group_id is a random number with a high probability of being unique.
// a request is an RPC call to either Server.Get or Server.Put
//
// a client that sends requests until the client learns one is successful from the server
//
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
//
// the approach most be more robust. RPC can be reilable meaning ok gives us a value. everytime or it can be
// unreable meaning  calls value cannot be relid on.this will change how part 6 is done and some other parts
// approach reliable first becuase that is what is familair.
//
// call is made to primary. it fails, then view service should be checked by client for most recent view, then that
// same job should be called again until a certian interval expires (must foind correct interval).
// this check should only be done once per ping intwerval to avoid burning p to much CPU time.
// so if the RPC call fails, contact view service. read most current views primary and
// make the same request again based on the information. now wait one ping interval. if found
// the RPC again failed repeat this process. client may loop forever or just fail aftyer an yet to be determined
// interval of time.
//
// this must run until successful
// ok = call(v.p, "Server.Put", PutArgs, PutReply)
//
// while !ok:
//	 v = vs.clerk.get()
//	 ok = call(v.p, "Server.Put", PutArgs, PutReply)
// 	 sleep(pingInterval)
//
//
// literally can just put in the the current client loop.
//
// what about unreliable? how does one know the RPC failed?
// how does one even know the primary is failing? it would be based on the information of the view service
// if the view service has a primary, the clients sends the request. if the server that reiceves
// the request replys back an error, then the client should get a new view, if no error is sent from the
// server (meaning the server the reqeust to is not actaully the primary and error should be sent), other
// wise the client assumes the primary recieved the message even though there is no garuntee.
//
// This means server.go needs a mechanism to send errors when a request is sent to server that is not primary
// check the lastest view
// this stops two active primaries
// v = vs.clerk.get()
//if !v.p:
//	call(client, "Client.Error")
//
//
