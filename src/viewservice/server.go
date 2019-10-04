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
	timeStamp time.Time
	name      string
	ttl       int
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
	currentView View
	// the view that the view service is currently using
	//Channel

	//reviece ping message data incoming reqeust. elimnates need to write to shared variable
	ping chan *SrvMsg
	//send replys from view service in form of mesage passing to elimante shared variables
	clientMsg chan View
}

//acquires lock when writing?

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	srvMsg := &SrvMsg{}
	//	srvMsg.timeStamp = time.Now()
	srvMsg.name = args.Me
	reply.View = vs.currentView
	vs.ping <- srvMsg
	vs.testCount += 1

	//	clientMsg := <-vs.clientMsg
	//	myLogger("2", "MSG: "+clientMsg.Primary, "Ping()", "ViewService.go")
	//	reply.View = clientMsg

	myLogger("3", "END: "+srvMsg.name, "Ping()", "ViewService.go")
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//retrieve the most current view

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	// THIS FUCNTION IS GOOD FOR PERIODIC DECISISONS
	// I.E to promote the backup, backup if the the view service has missed N pings (N = deadpings) from the primary
	// this fucntion Tick is called once per pinginterval

	srvMsg := <-vs.ping
	myLogger("2", "result: "+srvMsg.name+"ping count: "+strconv.Itoa(vs.testCount), "Tick()", "ViewService.go")
	if vs.currentView.Primary == "" && len(vs.servers) == 0 {
		vs.currentView.Primary = srvMsg.name
		vs.servers[srvMsg.name] = srvMsg
		myLogger("3", "Primary Elected: "+srvMsg.name, "Tick()", "ViewService.go")
	} else {
		myLogger("3", "No Election ", "Tick()", "ViewService.go")
	}

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

// View service is repsonsible for fault tolerance. it is a master server and a SPoF.
// it allows the p/b service to work correctly in the presence of network partions
// and decides which mahcines are alive
//
// GOAL OF LAB: to make a key/value service fault tolerant using a form of primary/backup replication.
//
// in order to ensure that all parties (clients and servers) agree on which server is the primary
// and which server is the backup, we introduce a kind of master server called the view service.
//
// note: k/v service consists of only two active servers and set of idle servers.
// the clients only access the primary and the primary keeps the backup in sync.
//
// the view service monitors whether each availible server is alive or dead.
// clients check with the view service to find the current primary
// (because in order for clients to use the key/value service they must contact the primary)
// the servers (not the clients) cooperate with the view service to ensure that at most
// one primary is active at a time.
//
// the key/value service allows replacement of failed servers.
// if the primary fails, the view service will promote the back up to be primary.
// if the back up fails, or is promoted. and there is an idle server availible ,
//  the view service will cause it to be the back up.
// the primary will send its complete database to the back up and then sends susbsequent puts to the backup
// to ensure that the backs ups key/value database remains identical to the primary.
//
// turns out primary must send Gets as well as Puts to the backup (if there is one).
// and must wait for the backup to reply before responding to the client. thsi prevents two severs
// from acting as the primary
//
// A key/value server can restart but will do so without a copy of the replicated data.
// (data is stored in main memory, not in disk)

// the view service goes through a sequence of numbered views  each with a primary and a backup if possible
// view consists of view number and an idetity for both e the primary and backup severs for the database.
//
// the primary in a view service must alwasy be eithe the primary or the back up of the previous view
// this helps ensure that the key value services state is preserved
// exception to the rule is when the view service first satarts should acpet any server at all
// as the first primary and the and the back up server can be any server other than the primary.
// or no server at all if none are availible reprsendt by ""
//
//key value servers will send pings once per ping interval.
// a ping lets the the view service know the

//the view service proceeds to a new view when either
//1. it hasnt recieved a ping from the primary or backup for N (N = Deadping) ping intervals,
//2. if the primary or back up crash or restart
//3. if there is no back up and there is an idle server
// (idle: a seerver thats ping that is niether primary or backup)
// view service MUST NOT change views (return a different view to callers) until
// the primary from the current view acknowlegdes that it is operating in the current view
// an akcnowdlegemnt is made by sending a ping with the current view number.
// view service should not change views if it has not recieved an ACK even if it thinks the primary is dead
// this ACK rule prevents the view service from getting more than one view a head of the key value servers
//down side of ACK rule is, if primary fails before it Acknowlegdes the view in whcih it is primary,
// then the view service cannot change views and spins forever. and cannot make forward progress.

// notes:
// 1. more than 2  servers may be sending pings, the extras (other than primary and backup)
// are volutneering to be bakc up if needed
// 2. the view service needs a way to detect that a primary or a back up has failed and restarted.
//  for example the primary may crash and quickly restart without missing a single ping.
// 3. make sure your server terminates correctly when the dead flag is set.  or you may fail tests

// Designing the Protocol
// Making a key/value service fault tolerant with p/b approach
//  clients (users of the k/v servive) make requests to the key/value service which contains the full database.
// the data base is kept in main memory on the server. therefore in the event of crash or
// resart our entire databse would be gone.
// To mitigate the risk of data loss, fault tolernace is introduced. we choose the primary/back up
// approach.
//
// the fault tolerant key/value sevrice consists of the
// two active servers, a set of idle servers, and the view service,
// the two active servers are the primary and the back up
// the job of the primary is to recieve requests from clients and keep the backup up to date
// the job of the back up is to recieve requests from the primary and keep an up to date copy of the database.
// the job of the idle servers is to volunteer to become a back up server.
// the job of the view service is to monitor what servers are alive and cooperate with the servers
// to agree on a single primary and back up for a given view.
// this means the view service, the active servers , and idle servers must work together to elect new active severs
//
// a view consists of a view number and the primary and back up server for that view
// a view represents an interval of time where a distinct primary and back up server ordering ocurrs.
// this logically seperates each failure/election.
// START:
//  when view service starts up its flag 'dead' should be set to false
//  the map should be initalized along with all other variables
//
// Intial Election:
//    - First Primary:
//       to elect the primary intiallly, the view service should accept any server at all;
//       that is, the first server to ping the view service.
//
//         - The very first ping, CK1.Ping(0) (0 is a view number indicating server restart usually after a crash)
//          the view service knows the server is Now alive,
//
//         tick is called once each ping interval
//         tick will recieve a time stamp from the channel. if the map is empty and there is no primary
//         tick will set the primary to the server that sent a message to the channel.
//         (do we want tick to block until it recieves?, it should be fine its on its own thread)

// - First Backup:
//        The second server to ping the view service becomes the back up,
//        if a second server never pings the back up, then the back up is set to ""
//
//   //ping for initial election
//   func ping():
//      reply = vs.currentView
//      ping <- srv,ts
//
//    func tick():
//      //initial election
//      srv, ts <- ping
//      //first primary
//      if v.p == "" && len(map) == 0:
//           v.p = srv
//           map[srv] = ts
//      //first backup
//      else if v.b == "" && len(map) == 1 && !map.contains(srv):
//          v.b = srv
//          map[srv] == ts
//      //idle server
//      else:
//          map[srv] = ts
//
//   //detecting failed servers
//     if number of servers is greater than deadPing const potential for server to be dectected as dead
//     when server is actually alive.
//
//    if we recieve a value from the from the channel
///    that servers TTL count in the map is set to 0
//     map<srv,(ts, ttl)>
//  func ping():
//      reply = vs.currentView
//      ping <- srv,ts
//
//    func tick():
//      //initial election
//      srv, ts <- ping
//      //first primary
//      if v.p == "" && len(map) == 0:
//           v.p = srv
//           map[srv] = (ts, DeadPingTTL)
//      //first backup
//      else if v.b == "" && len(map) == 1 && !map.contains(srv):
//          v.b = srv
//          map[srv] == (ts, DeadPingTTL)
//      //idle server
//      else:
//          map[srv] = (ts, DeadPingTTL)

//  //decrement TTL counter for all srvs that did not recieve a ping,
//  for key in map:
//     if key != srv:
//      srvPair = map[key]
//      map[key] = (srvPair[0], srvPair[1] - 1)
//
//   //remove dead srvs from map
//   for key in map:
//      //detect dead srv
//      if map[key][1] == 0:
//          if v.p == key
//            map.remove(key)
//            PromoteBackupToPrimary()
//            PromoteIdleToBackUp()
//
//
//
// if srv is primary, and is failed, promote back up
//     - When a ping is called, the map is updated.
//                 - there may be a race condition ,because multiple pings can arrive from different servers
//                    at the same time each writing an entry to the map. The view service reads from the map
//                 - see if there is a way to simplify with channels, however there is a mutex lock declared.
//                  which implies it should be used
//
//
//
// Primary Fails:
//    - Primary Takes Over For Backup:
//        if the primary fails, the back up should be promoted to a primary.
//
//    - Restarted Server Becomes Backup:
//        server that crashed then restarted is elected backup. server is back and is now idle.
//
//    - Idle Third Server Becomes Backup:
//        if a idle server exists, then the idle server should be promoted to backup.
//        if no idle servers exist, there is no backup. (the backup is set to the empty string "")
//
//    -  Restarted Primary Treated as Dead
// Acknowlegdements:
//    - View Server Waits For Primary To Ack View:
//    - Unitilaized Server Cannot Become Primary:
