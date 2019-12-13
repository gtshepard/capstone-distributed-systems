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
	for key := range vs.servers {
		if vs.servers[key].ttl <= 0 {
			//if primary not alive
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
}

// View Service
// goal of view service
// 1. assign roles
// 2. make sure servers are alive
//
// Servers in K/V Service ping View Service
// View service replys to server requests with view objects
// View objects contain server roles and view number
//
// the view service has a current view number, the current view number that can be learned
// no other view can be learned. the currentview number only increases, never decreases
//
// case 1: start up
//   upon start up, the view service has a current view number of 1
//   no K/V servers are present upon start up, therefore no pings, therefore no roles assigned
//
// case 2: n servers ping when no roles assigned
// 	 case 2a) elect primary
//	 	 the first server S_i to successfully ping the view service will be assigned the role of primary
//		 the very first assignment of primary does not increase the current view number. (this is a special case)
//
//   case 2b) elect backup
//  		all n-1 servers left that have also sent pings, will reach the View Service
//   		the first server of these n-1 servers, S_k to successfully ping the View Service
//   		will be elected backup, all remaining n-2 servers will eventually ping the view service,
//   		and will not be assigned a role.
//
//	 		this idea leads to two cases because of the following
//
//  		a backup should NOT be assgined until the primary has acknowledged the current
//  		view that it is in. once the primary has acknowleged this view, only then the backup can be elected.
//	 		we say the S_k will eventually be elected backup
//
//
//   		2b_1) S_k makes a successful ping after the primary P has acknowleged the current view
//				S_k is elected backup. the current view number is increased by one for the role change
//				any server who sends a ping from this point will learn the new view.
//				the primary P will learn this view from its next succesful ping p_k. the primary P will acknowledge the
//				current view with a succesful ping p_k+1
//
//    		2b_2) S_k makes a successful ping and the primary P has NOT yet acknowleged the current view
//
//        		role assignment of backup to S_k must be delayed until primary acknowledges the backup
//
//       	 	We say S_k will eventually be elected backup
//        		to ensure S_k is eventually elected backup, all pings from all servers (except primary)
//        		are recorded in a slice with a time stamp and the server name. These records can later be accessed.
//        		the record will be accessed when the primary P  acknowledges the current view V it is currently operating in
//				(before backup sent ping)

//
//       		once the primary acknowlegdes the current view V, the slice containing the records of pings should
//        		be sorted in DESC order by time stamp, then the first item in the slice should be selected
//        		which will be the message sent by S_k. (not bad nLogn once every view change)
//
//        		with this information the backup can now be elected, and the current view number
//        		should now increase by 1, servers can immedaitely start learning the new view
//				the successful ping cache is cleared (the slice containing the records).
//
//				any time there is a new current view V, the primary must learn this current view V  ands need to acknowledge it
//				any time the primary must acknowldege a view, there is a chance that some server S_m will attempt to become backup
//				there for it is crucial that we ensure evenutal election of the backup, not immedaite, in the case where the primary
//				has not yet acknowledgeed the current view V
//
//        		a backup role assignment, is considered a change in the view.
//        		therefore the view number will be incremented. any server that makes a request
//        		to the view service will recieve this view from now on.
//
//        		response:
//	        		p: S1
//          		b: S2
//          		v_n: 2
//		case 2c) idle servers ping after roles of primary and backup have been assigned
//			no role is assigned. idle servers make pings with in given time bound (pinginterval) to let
//			the view service know that there are still alive and ready to be elected if need be
//
//
//  X case 3: n servers ping when, primary already assigned
//     a backup will be elected. for this process see case 2b
//
//  X case 4: n servers ping when back up already assigned
//		all roles have been already been assigned. see case 2c)
//
//  X case 5: primary fails, with backup, n idle servers
//
//	    consider when the primary P fails and with an availible backup B.
//	    server failure is detected by the view service.
//
//	    a server is considered failed if the view service does not hear from
//      a server in deadping pingintervals. meaning if a message is not sent with
//	    in the expected time bound T, within N number of times (where M = deadping) the server
//	    is consisered failed.
//
//     5a) primary P fails before P acknowledges the current view V
//          view service will spin for ever, service is not expected to work correctly for this case
//
//	   5b) primary P fails after P has acknowleged current view V
//		    if Primary P fails, the backup B for the current view is promoted to the role of primary
//          and the current view number is increased by 1 for role change.
//
//			observe the following
//
//			5b_1) backup promoted, n idle where n = 0
//              the new primary (previously the backup B) will learn the new view next time P sends a succucessful ping p_k.
//				a successful ping is a ping that reaches the view service. the next successful ping p_k+1 sent by the primary
//			    will act as the primary's acknowledgement of the current view there are no idle servers to take on the role of the backup
//
//			5b_2) backup promoted, n idle where n > 0
//				  previously backup B is now the primary and there are n idle servers that could pontentially
//				  become the new backup.
//
//			      5b_2_a) new primary (previously backup B) HAS acknowleged the current view
//					the first server S_i to ping to succesfully ping the view service is elected
//					as the new backup. the current view number is increased by 1 for role change.
//					the new view can be learned immediately. the new primary will learn of this current view V with its next
//					successful ping p_k. the new primary will acknoweledge the current view with successful ping p_k+1
//
//				  5b_2_a) new primary (previously backup B) has NOT acknowleged the current view
//				     to elect back up with these constraints follow case 2b
//
//
//  case 6: backup fails, n idle servers
//      	a primary P and a backup B are operating in the current view. n idle servers that could potenitally
//			become backup
//
//		   6a) backup B fails, n = 0
//				no idle servers to take place. no elections
//				current view number is increased. dead backup is removed
//			    new view is availible to be learned
//
//		   6b) backup B fails, n > 0
//				a new backup must be selected. see 3b_2)  for the procedure
//				note that there is no new primary, but the same cases present themselves
//
//
// case 7: primary fails, no backup assigned, no idles servers availbile
//		service fails. all data is lost. system desgined to handle N-1 faults and where N is the number of servers that make up the service.
//		this N excludes the view service. it is assumed the view service does NOT fail. in the event of the view service
//		failure, the system is not expected to work correctly
//
//
//
//

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
