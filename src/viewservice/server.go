package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type SrvMsg struct {
	timeStamp  time.Time
	name       string
	ttl        int
	oldViewNum uint
}

type ViewServer struct {
	mu        sync.Mutex
	l         net.Listener
	dead      bool
	me        string
	testCount int
	// Your declarations here.
	//MAP OF MOST RECENT TIMES VIEW SERVICE HAS HEARD A PING FROM EACH SERVER
	//Map where the key is the server and the value is the time stamp of the
	// most recent ping heard from the server
	servers map[string]*SrvMsg
	//CURRENT VIEW
	currentView     View
	isFirstElection bool
	// the view that the view service is currently using
	//Channel

	//reviece ping message data incoming reqeust. elimnates need to write to shared variable
	ping chan *SrvMsg
	//send replys from view service in form of mesage passing to elimante shared variables
	clientMsg          chan View
	crashAndRestart    uint
	primaryAcknowleged bool
	viewHasChanged     bool
	backupFailed       bool
	deadServers        map[string]*SrvMsg
	flushInterval      int
	intervalCount      int
}

//acquires lock when writing?

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	srvMsg := &SrvMsg{}
	srvMsg.timeStamp = time.Now()
	srvMsg.name = args.Me
	srvMsg.oldViewNum = args.Viewnum

	//send reply
	reply.View = vs.currentView
	//send message to Ticker thread

	vs.ping <- srvMsg
	//myLogger("3", "END OF PING CALL"+srvMsg.name+": "+t, "Ping()", "ViewService.go")
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//retrieve the most current view
	reply.View = vs.currentView
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	srvMsg := <-vs.ping

	if vs.intervalCount >= vs.flushInterval {
		vs.intervalCount = 0
		for k := range vs.deadServers {
			delete(vs.deadServers, k)
		}
	}

	if val, ok := vs.deadServers[srvMsg.name]; ok {
		myLogger("------------", "STRAY PING FROM DEAD SERVER : "+val.name, "------------", "ViewService.go")
		return
	}
	myLogger("3", "VS--SRV--PINGGEd", srvMsg.name, "ViewService.go")
	//has primary acknowdleged current view
	for key := range vs.servers {
		vs.servers[key].ttl -= 1
		ttl := strconv.Itoa(vs.servers[key].ttl)
		myLogger("3", "TTL FOR SRV : "+key+":"+ttl, "Tick()", "ViewService.go")
	}

	if srvMsg.name == vs.currentView.Primary {
		if srvMsg.oldViewNum == vs.currentView.Viewnum {
			vs.primaryAcknowleged = true
			myLogger("", "PRIMARY ACKED", srvMsg.name, "ViewService.go")
		}
	}

	if vs.currentView.Primary == "" && vs.isFirstElection {
		//vs.currentView.Viewnum += 1
		vs.currentView.Primary = srvMsg.name
		//vs.currentView.JustElectedBackup = false
		vs.servers[srvMsg.name] = srvMsg
		vs.servers[srvMsg.name].ttl = DeadPings
		vs.isFirstElection = false
		myLogger("", "ELECTED FIRST PRIMARY", srvMsg.name, "ViewService.go")

	} else if vs.currentView.Backup == "" && srvMsg.oldViewNum == vs.crashAndRestart {
		//make sure primary has acked for currwent  view
		if srvMsg.name != vs.currentView.Primary {
			if vs.primaryAcknowleged {
				vs.currentView.Backup = srvMsg.name
				//vs.currentView.JustElectedBackup = true
				vs.servers[srvMsg.name] = srvMsg
				vs.servers[srvMsg.name].ttl = DeadPings
				vs.primaryAcknowleged = false
				vs.currentView.Viewnum += 1
				myLogger("", "ELECTED BACKUP RESTARTED: "+srvMsg.name, "Tick()", "ViewService.go")
			} else {
				myLogger("", "UNACKED: ", "Tick()", "ViewService.go")
			}
		} else {
			//account for primary pinging before first backup elected
			myLogger("", "BAD: "+srvMsg.name, "Tick()", "ViewService.go")
			//vs.currentView.JustElectedBackup = false
			vs.servers[srvMsg.name].ttl = DeadPings
		}

	} else if srvMsg.name == vs.currentView.Primary && srvMsg.oldViewNum == vs.crashAndRestart {
		myLogger("", "PRIMARY RESTART "+srvMsg.name, "Tick()", "ViewService.go")
		//	vs.currentView.JustElectedBackup = false
		vs.primaryAcknowleged = true
		//treat primary restart as dead
		vs.servers[srvMsg.name].ttl = 0
	} else {
		//	vs.currentView.JustElectedBackup = false
		vs.servers[srvMsg.name] = srvMsg
		vs.servers[srvMsg.name].ttl = DeadPings
		dp := strconv.Itoa(DeadPings)
		myLogger("3", "RESTORE TTL  : "+srvMsg.name+":"+dp, "Tick()", "ViewService.go")

		if vs.currentView.Backup == "" && vs.backupFailed && srvMsg.name != vs.currentView.Primary {
			myLogger("@@@@@@@@@@@@", "REPLACE FAILED BACKUP  : "+srvMsg.name, "Tick()", "@@@@@@@@@@@@")
			vs.currentView.Viewnum += 1
			vs.currentView.Backup = srvMsg.name
			//vs.currentView.JustElectedBackup = true
			vs.backupFailed = false
		} else if vs.currentView.Backup == "" && srvMsg.name != vs.currentView.Primary {
			myLogger("@@@@@@@@@@@@", "ADD BACKUP IDLE - NO INIT BACKUP  : "+srvMsg.name, "Tick()", "@@@@@@@@@@@@")
			vs.currentView.Viewnum += 1
			vs.currentView.Backup = srvMsg.name
		}
	}

	// //promote back up and prune dead servers
	for key, value := range vs.servers {
		if vs.servers[key].ttl <= 0 {
			//if primary not alive
			vs.deadServers[key] = value
			if key == vs.currentView.Primary {
				myLogger("", "PRIMARY FAILED: "+key, "Tick()", "ViewService.go")
				//wait for ack to change views
				if vs.primaryAcknowleged {
					vs.currentView.Viewnum += 1
					vs.currentView.Primary = vs.currentView.Backup
					myLogger("", "PROMOTED TO PRIMARY: "+vs.currentView.Backup, "Tick()", "ViewService.go")
					vs.currentView.Backup = ""
					//vs.currentView.JustElectedBackup = false
					delete(vs.servers, key)
					vs.primaryAcknowleged = false
				} else {
					myLogger("@@@@@@@@@@", "PRIMARY IS BEHIND CURRENT VIEW : "+key, "T", "@@@@@@@@@@@@")
				}
			} else if key == vs.currentView.Backup {
				myLogger("", "BACKUP FAILED: "+vs.currentView.Backup, "Tick()", "ViewService.go")
				vs.currentView.Backup = ""
				//	vs.currentView.JustElectedBackup = false
				delete(vs.servers, key)
				vs.primaryAcknowleged = false
				vs.backupFailed = true
			}
		}
	}

	for key := range vs.servers {
		if vs.servers[key].ttl <= 0 {
			if key != vs.currentView.Primary {
				delete(vs.servers, key)
			}
		}
	}
	vs.intervalCount += 1
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.ping = make(chan *SrvMsg)
	vs.clientMsg = make(chan View)
	vs.servers = make(map[string]*SrvMsg)
	vs.testCount = 0
	vs.currentView.Viewnum = 1
	vs.isFirstElection = true
	vs.crashAndRestart = 0
	vs.primaryAcknowleged = false
	vs.backupFailed = false
	vs.flushInterval = 5
	vs.intervalCount = 0
	vs.deadServers = make(map[string]*SrvMsg)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	//set up a listener that waits   for incoming requests.each
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		//while view service is alive
		for vs.dead == false {
			//recieves incoming connections
			conn, err := vs.l.Accept()
			//if no errors and the view service is not dead
			if err == nil && vs.dead == false {
				//handle requests for this connection on a seperate thread
				go rpcs.ServeConn(conn)
			} else if err == nil {
				//close connection
				conn.Close()
			}
			//if there are errors
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()
	return vs
}
