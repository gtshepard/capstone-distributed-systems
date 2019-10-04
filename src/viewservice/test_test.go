package viewservice

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

//t *testing.T: a testing framework
// ck *Clerk: a clerk object (k/v server)
// p string: a primary
// b string: a backup
// n unit: view number
func check(t *testing.T, ck *Clerk, p string, b string, n uint) {
	//get clerk info
	view, _ := ck.Get()
	//make assertions
	if view.Primary != p {
		t.Fatalf("wanted primary %v, got %v", p, view.Primary)
	}
	if view.Backup != b {
		t.Fatalf("wanted backup %v, got %v", b, view.Backup)
	}
	if n != 0 && n != view.Viewnum {
		t.Fatalf("wanted viewnum %v, got %v", n, view.Viewnum)
	}
	if ck.Primary() != p {
		t.Fatalf("wanted primary %v, got %v", p, ck.Primary())
	}

}

func port(suffix string) string {
	//make port name
	s := "/var/tmp/824-"
	//get user-id
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	//aappend viewsevrer suffix
	s += "viewserver-"
	//append PID
	s += strconv.Itoa(os.Getpid()) + "-"
	//appened unique suffix
	s += suffix
	return s
}

func Test1(t *testing.T) {
	//number of CPU cores program will use
	runtime.GOMAXPROCS(4)
	//port name for view service sever (the master server)
	vshost := port("v")
	//creates view service sever (the master server)
	vs := StartServer(vshost)

	//makes k/v servers and denot the view service as there master
	ck1 := MakeClerk(port("1"), vshost)
	ck2 := MakeClerk(port("2"), vshost)
	ck3 := MakeClerk(port("3"), vshost)

	//a primary was assinged too soon. no primary should be elected yet
	if ck1.Primary() != "" {
		t.Fatalf("there was a primary too soon")
	}

	// very first primary
	fmt.Printf("Test: First primary ...\n")

	for i := 0; i < DeadPings*2; i++ {
		// primary should be elected. times out after 10 attempts
		// kv server 1 restarted
		view, _ := ck1.Ping(0)

		//verifies priamry has been elected
		if view.Primary == ck1.me {
			myLogger("T-Primary", "IS PRIMARY: "+ck1.me, "test1", "test_test.go")
			break
		}

		//ping called once per ping interval
		time.Sleep(PingInterval)
	}

	//test assertion, is primary
	check(t, ck1, ck1.me, "", 1)
	fmt.Printf("  ... Passed\n")

	// very first backup
	fmt.Printf("Test: First backup ...\n")
	//explicitly define scope with code block
	{
		//get the primaries info for the check
		vx, _ := ck1.Get()
		for i := 0; i < DeadPings*2; i++ {
			//ping primary
			ck1.Ping(1)
			// back up should be elected. times out after 10 attempts
			// k/v server 2 restarted.
			// pings do 3 things
			// 1. tell view service that it that pinging k/v server is alive
			// 2. informs the k/v server kf the current view
			// 3. informs the view service of the most recent view the k/v service knows about
			view, _ := ck2.Ping(0)
			//verify backup has been elected
			if view.Backup == ck2.me {
				break
			}
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//test assertion, is backup
		check(t, ck1, ck1.me, ck2.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")
	// primary dies, backup should take over
	fmt.Printf("Test: Backup takes over if primary fails ...\n")

	{
		// view  number 2 this is the most recent view the k/v 1 server knows about
		ck1.Ping(2)
		// view  number 2 this is the most recent view the k/v 2 server knows about
		//vx is latest view, (learns from the view service )
		vx, _ := ck2.Ping(2)
		//
		for i := 0; i < DeadPings*2; i++ {
			v, _ := ck2.Ping(vx.Viewnum)
			//the back up now will now be the primary. and there is no back up
			if v.Primary == ck2.me && v.Backup == "" {
				break
			}
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//test assertion, backup did take over for primary
		check(t, ck2, ck2.me, "", vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Restarted server becomes backup ...\n")

	{
		//note pings keep the severs in sync, so when evetns happend like a sever crash,
		//the servers will perform taks beased on the repsone or lack of reposne form th pjngs

		//get the latest view k/v server knows about
		vx, _ := ck2.Get()
		//ping the view service. pings do 3 things
		//1. informs view service that k/v server is stil alive
		//2. informs the view service of the most recent view the k/v server knows about
		//3. k/v server learns the newest view from the View Service
		ck2.Ping(vx.Viewnum)

		for i := 0; i < DeadPings*2; i++ {
			//indicates server crash or restart
			ck1.Ping(0)

			//pings the view service, pings do 3 things
			//1. informs the view service that k/v server is still alive
			//2. informs the view service of the most recent view the k/v server knows about
			//3. k/v server learns the newest view from the view service.
			v, _ := ck2.Ping(vx.Viewnum)
			//restarted server is elected as back up, (at most ten attempts)
			if v.Primary == ck2.me && v.Backup == ck1.me {
				break
			}
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//test assertion, restarted did become backup
		check(t, ck2, ck2.me, ck1.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// start ck3, kill the primary (ck2), the previous backup (ck1)
	// should become the server, and ck3 the backup
	fmt.Printf("Test: Idle third server becomes backup if primary fails ...\n")

	{
		//get most recent view for k/v primary
		vx, _ := ck2.Get()
		//ping view service, pings do 3 things
		//1. informs the the view service the k/v server is still alive
		//2. informs the view service of the most recent view the k/v server knows about
		//3. k/v server learns the newest view from the view service
		//remember CK2 is primary at this point
		ck2.Ping(vx.Viewnum)

		for i := 0; i < DeadPings*2; i++ {
			//restart the idle server
			ck3.Ping(0)
			//pings do 3 things
			//1. informs the view service the k/v server is still alive
			//2. informs the view service of the most recent view the k/v server knows about
			//3. k/v server learns the newest view from the view service
			//CK1 is the backup in the beginning, becimes primary here
			v, _ := ck1.Ping(vx.Viewnum)

			//CK1 has been elected primary, and CK3 has been elected backup
			if v.Primary == ck1.me && v.Backup == ck3.me {
				break
			}
			vx = v
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//test assertion, back up did become primary and idle did become backup
		check(t, ck1, ck1.me, ck3.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// kill and immediately restart the primary -- does viewservice
	// conclude primary is down even though it's pinging?
	fmt.Printf("Test: Restarted primary treated as dead ...\n")

	{
		//reritve primary k/v view information
		vx, _ := ck1.Get()
		//ping the view view service, pings do 3 things
		//1. inform the view service the that the k/v server is still alive
		//2. inform the view service of the most recent view the k/v server knows about
		//3. k/v server learns the  newest view from the view service
		ck1.Ping(vx.Viewnum)

		for i := 0; i < DeadPings*2; i++ {
			//restart primary
			ck1.Ping(0)

			// backup sends ping to view service, pings do 3 things
			// 1. inform the view service that the key value service is still alive
			// 2. inform the view service of the most recent view the k/v server knows about
			// 3. k/v server learns the newest view from the view service
			ck3.Ping(vx.Viewnum)

			//get view information from view service
			v, _ := ck3.Get()
			//ck1 is not longer primary, treated as dead.
			if v.Primary != ck1.me {
				break
			}
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		// get view information from view service
		vy, _ := ck3.Get()
		//test assertion CK3 did become primary
		if vy.Primary != ck3.me {
			t.Fatalf("expected primary=%v, got %v\n", ck3.me, vy.Primary)
		}
	}
	fmt.Printf("  ... Passed\n")

	// set up a view with just 3 as primary,
	// to prepare for the next test.
	{
		for i := 0; i < DeadPings*3; i++ {
			//get view information from view service
			vx, _ := ck3.Get()
			//ping view service (THIS MIGHT BE AN ACK)
			ck3.Ping(vx.Viewnum)
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//get view info from view service
		v, _ := ck3.Get()
		//test asserition CK3 did become primary with no backup
		if v.Primary != ck3.me || v.Backup != "" {
			t.Fatalf("wrong primary or backup")
		}
	}

	// does viewserver wait for ack of previous view before
	// starting the next one?
	fmt.Printf("Test: Viewserver waits for primary to ack view ...\n")

	{
		// set up p=ck3 b=ck1, but
		// but do not ack
		vx, _ := ck1.Get()

		for i := 0; i < DeadPings*3; i++ {
			//back up resetarts or crashed
			ck1.Ping(0)
			//CK3 is primary
			//pings do 3 things
			//1. informs view service server is still alive
			//2. informs view service of most recent view k/v server knows about
			//3. k/v server learns the newest view from view service
			ck3.Ping(vx.Viewnum)
			//get view information from view service
			v, _ := ck1.Get()
			//fails if on a further along view?
			if v.Viewnum > vx.Viewnum {
				break
			}
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//test assertion, view service did wait for ACK
		check(t, ck1, ck3.me, ck1.me, vx.Viewnum+1)
		vy, _ := ck1.Get()
		// ck3 is the primary, but it never acked.
		// let ck3 die. check that ck1 is not promoted.
		for i := 0; i < DeadPings*3; i++ {
			//pings do 3 things
			//1. informs view service that server is still alive
			//2. informs view service of the most recent view k/v server knows about
			v, _ := ck1.Ping(vy.Viewnum)
			//if CK1 is promoted break (we do not want it to be )
			if v.Viewnum > vy.Viewnum {
				break
			}
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}
		//test assseriton CK1 was not promoted
		check(t, ck2, ck3.me, ck1.me, vy.Viewnum)
	}
	fmt.Printf("  ... Passed\n")

	// if old servers die, check that a new (uninitialized) server
	// cannot take over.
	fmt.Printf("Test: Uninitialized server can't become primary ...\n")
	{
		for i := 0; i < DeadPings*2; i++ {
			//get view info from view service
			v, _ := ck1.Get()
			//ping does 3 things
			//1. informs view service that server is still alive
			//2. informs view service of most recent view that k/v server knows about
			//3. k/v learns the newest view from the view service
			ck1.Ping(v.Viewnum)
			//CK2 restarted or crashed
			ck2.Ping(0)
			//pings view service. ping does 3 things
			//1. informs view service that server is still alive
			//2. informs view service of most recent view that k/v server knows about
			//3. k/v server learns the newest view from the view service
			ck3.Ping(v.Viewnum)
			//severs send a  ping once per ping interval
			time.Sleep(PingInterval)
		}

		for i := 0; i < DeadPings*2; i++ {
			//CK2 restrtes or crahes 10 times
			ck2.Ping(0)
			time.Sleep(PingInterval)
		}
		//get most recent view info from view service
		vz, _ := ck2.Get()
		//should not have  CK2 as pimary or test fails
		if vz.Primary == ck2.me {
			t.Fatalf("uninitialized backup promoted to primary")
		}
	}
	fmt.Printf("  ... Passed\n")
	//kill view service
	vs.Kill()
}

// the view service is comprised of a sequence of numbered views. Each with a primary and back up
// (primary and a back up are both servers that host the key/value service)
// a view consists of a view number and the identity of the primary and back up servers.
//  an identity is a network port name .
// The primary in a view must always be either the primary or the backup of the previous view
// (what is a previous view)? does this mean there is only one active view at a time? Yes the current view is the "active voew"
// Each Key/Value sever should send a ping RPC once per ping interval.
// the view service replies with a description of the current view  (the information the view holds identity and view number)
//  the above ping does 3 thingd
// 1. informs view service that k/v server is still alive
// 2. informs k/v serverof the current view
// 3. informs View Service of most recent view that the k/v server knows about
//
// There are 3 cases when the view service switches to a new view
// 1. Has not recieved a ping from a primary or backup (for dead ping intervals)
// 2. Primary or Backup crashes or is restarted
// 3. There is no Back up and there is an idle server (a server that has been ping but is neither primary or back up)
//
// The case where view service should NOT change views
// the view service must NOT change views until the primary from the current view acknoweldegs that is operating as the current view
// an ackanledgement is sending back the a ping with the current view number
// if the view service has not yet recieved  an ACK for the current view from the primary of the current view, the view service should not
// chnange views even if it it thinsk the primary or backup has died.
// ACK prevents view service from ever gettign more than one a head of the k/v servers
// if primary fails before it ACKS the view for whoch it is primary,
// then the view service cannot chane evieews an loops forever makes no progres
