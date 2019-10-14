package pbservice

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
	"viewservice"
)

//check if k/v service grabs the correct value
func check(ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		myLogger("ERROR", v, "TT", "TT")
		log.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
	myLogger("FINISH", v, "TT", "TT")
}

//create port for server to establish connection
func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "pb-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

//TEST 1
func TestBasicFail(t *testing.T) {
	runtime.GOMAXPROCS(4)
	//why this tag?
	tag := "basic"
	//creates view service
	vshost := port(tag+"v", 1)
	//starts view service (imported the view service package)
	vs := viewservice.StartServer(vshost)
	//wait for 1 second
	time.Sleep(time.Second)

	vck := viewservice.MakeClerk("", vshost)
	//create a client that uses the KV service
	ck := MakeClerk(vshost, "")

	fmt.Printf("Test: Single primary, no backup ...\n")
	// create a k/v server
	s1 := StartServer(vshost, port(tag, 1))
	//why this specific interval of time to wait?
	deadtime := viewservice.PingInterval * viewservice.DeadPings
	//give view service enough time to elect primary
	time.Sleep(deadtime * 2)
	if vck.Primary() != s1.me {
		t.Fatal("first primary never formed view")
	}
	//insert value into key/value data base
	ck.Put("111", "v1")
	//check to see if the database the correct value
	check(ck, "111", "v1")
	myLogger("Passed First Put", "PASS", "TT", "TT")
	ck.Put("2", "v2")
	check(ck, "2", "v2")

	ck.Put("1", "v1a")
	check(ck, "1", "v1a")

	fmt.Printf("  ... Passed\n")
	// add a backup
	fmt.Printf("Test: Add a backup ...\n")
	//create backup k/v server
	s2 := StartServer(vshost, port(tag, 2))

	for i := 0; i < viewservice.DeadPings*2; i++ {
		//get currnt view
		v, _ := vck.Get()
		//if equal view service elected bakcup after a series re
		if v.Backup == s2.me {
			break
		}
		//1 iteration takes ping interval time
		time.Sleep(viewservice.PingInterval)
	}
	//no back up elected
	v, _ := vck.Get()
	if v.Backup != s2.me {
		t.Fatal("backup never came up")
	}
	//add a key and value to database
	ck.Put("3", "33")
	//verfiy correct dta was stored in database
	check(ck, "3", "33")

	// give the backup time to initialize
	time.Sleep(3 * viewservice.PingInterval)
	//put key/value onto data base
	ck.Put("4", "44")
	//verify correct data was stored in the data base
	check(ck, "4", "44")

	fmt.Printf("  ... Passed\n")
	// kill the primary
	fmt.Printf("Test: Primary failure ...\n")
	//primary fails
	s1.kill()
	//wait for backup to be elected
	for i := 0; i < viewservice.DeadPings*2; i++ {

		v, _ := vck.Get()
		//backup elected
		if v.Primary == s2.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	v, _ = vck.Get()
	//no backup elected
	if v.Primary != s2.me {
		t.Fatal("backup never switched to primary")
	}

	//verfiy state of k/v is perserverd after backup is promoted
	//i.e did backup have the full copy of the database
	check(ck, "1", "v1a")
	check(ck, "3", "33")
	check(ck, "4", "44")

	fmt.Printf("  ... Passed\n")

	// kill solo server, start new server, check that
	// it does not start serving as primary

	fmt.Printf("Test: Kill last server, new one should not be active ...\n")
	s2.kill()
	//start a new server that should not be come active
	s3 := StartServer(vshost, port(tag, 3))
	time.Sleep(1 * time.Second)
	get_done := false
	go func() {
		ck.Get("1")
		get_done = true
	}()
	time.Sleep(2 * time.Second)
	if get_done {
		t.Fatalf("ck.Get() returned even though no initialized primary")
	}

	fmt.Printf("  ... Passed\n")
	//kill all servers and view service at end of test
	s1.kill()
	s2.kill()
	s3.kill()
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

//TEST 2
func TestAtMostOnce(t *testing.T) {
	runtime.GOMAXPROCS(4)
	//not sure whatt this tag means
	tag := "csu"
	vshost := port(tag+"v", 1)
	//start view service
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	//make view service worker
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: at-most-once Put; unreliable ...\n")
	//start 1 server
	const nservers = 1
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		myLogger("1", "THIS IS ITER: "+strconv.Itoa(i), "TestAtMostOnce", "test_test")
		sa[i] = StartServer(vshost, port(tag, i+1))
		sa[i].unreliable = true
	}

	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		//why is this here?
		if view.Primary != "" && view.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(viewservice.PingInterval * viewservice.DeadPings)

	ck := MakeClerk(vshost, "")
	k := "counter"
	val := ""
	//make sure  keys match values
	for i := 0; i < 100; i++ {

		v := strconv.Itoa(i)
		//computes the new value for a key using the hash function in common.go and insert the k/v pair
		//into the database where the value is a hash.
		//first value is possibly  ""
		//keep inserting values under the same key, each value has a uniqye hash when in sertted.
		pv := ck.PutHash(k, v)

		if pv != val {
			t.Fatalf("ck.Puthash() returned %v but expected %v\n", pv, val)
		}
		//checks to make sure value was hashed correctly
		h := hash(val + v)
		val = strconv.Itoa(int(h))
	}
	//should be most recent value inserted into database
	//at most once semtantics ensure becuase there was 100 puthash calls, and each call retries
	//until succeds this means duplicate calls will be made. must filter out duplicates
	//and ensure the last value to be inserted is actually inserted, otherwise some other duplicat e
	//interrupted and overrided the expected last value
	v := ck.Get(k)
	if v != val {
		t.Fatalf("ck.Get() returned %v but expected %v\n", v, val)
	}

	fmt.Printf("  ... Passed\n")
	//shut down k/v service
	for i := 0; i < nservers; i++ {
		sa[i].kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

//TEST 3
// Put right after a backup dies.
func TestFailPut(t *testing.T) {
	runtime.GOMAXPROCS(4)
	//put executed rigtj after a primary or backup dies should still succed once new primary
	//or back up is slectled. back should still contain identical copy of primary

	tag := "failput"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	//make vs worker handles, requests
	vck := viewservice.MakeClerk("", vshost)
	//create P, B, and idle server
	s1 := StartServer(vshost, port(tag, 1))
	time.Sleep(time.Second)
	s2 := StartServer(vshost, port(tag, 2))
	time.Sleep(time.Second)
	s3 := StartServer(vshost, port(tag, 3))

	//wait for election
	for i := 0; i < viewservice.DeadPings*3; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	time.Sleep(time.Second) // wait for backup initializion

	//esnure S1 elected primary, S2 elected backup
	v1, _ := vck.Get()
	if v1.Primary != s1.me || v1.Backup != s2.me {
		t.Fatalf("wrong primary or backup")
	}
	//make client that will use database
	ck := MakeClerk(vshost, "")
	//client adds data to database
	ck.Put("a", "aa")
	ck.Put("b", "bb")
	ck.Put("c", "cc")
	check(ck, "a", "aa")
	check(ck, "b", "bb")
	check(ck, "c", "cc")

	// kill backup, then immediate Put
	fmt.Printf("Test: Put() immediately after backup failure ...\n")
	s2.kill()
	ck.Put("a", "aaa")
	check(ck, "a", "aaa")

	for i := 0; i < viewservice.DeadPings*3; i++ {
		v, _ := vck.Get()
		if v.Viewnum > v1.Viewnum && v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	time.Sleep(time.Second) // wait for backup initialization
	v2, _ := vck.Get()
	if v2.Primary != s1.me || v2.Backup != s3.me {
		t.Fatal("wrong primary or backup")
	}

	check(ck, "a", "aaa")
	fmt.Printf("  ... Passed\n")

	// kill primary, then immediate Put
	fmt.Printf("Test: Put() immediately after primary failure ...\n")
	s1.kill()
	ck.Put("b", "bbb")
	check(ck, "b", "bbb")

	//make sure the new view that has elected primary or else fialed swevrer
	for i := 0; i < viewservice.DeadPings*3; i++ {
		v, _ := vck.Get()
		if v.Viewnum > v2.Viewnum && v.Primary != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	time.Sleep(time.Second)

	check(ck, "a", "aaa")
	check(ck, "b", "bbb")
	check(ck, "c", "cc")
	fmt.Printf("  ... Passed\n")

	s1.kill()
	s2.kill()
	s3.kill()
	time.Sleep(viewservice.PingInterval * 2)
	vs.Kill()
}

//TEST 4
// do a bunch of concurrent Put()s on the same key,
// then check that primary and backup have identical values.
// i.e. that they processed the Put()s in the same order.
func TestConcurrentSame(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "cs"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: Concurrent Put()s to the same key ...\n")

	const nservers = 2
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
	}
	//elect primary and backup or failure
	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(viewservice.PingInterval * viewservice.DeadPings)

	done := false

	view1, _ := vck.Get()
	const nclients = 3
	const nkeys = 2
	for xi := 0; xi < nclients; xi++ {
		go func(i int) {
			ck := MakeClerk(vshost, "")
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa(rr.Int() % nkeys)
				v := strconv.Itoa(rr.Int())
				ck.Put(k, v)
			}
		}(xi)
	}
	//two clients make requests concurrently to the same key for 5 seconds
	time.Sleep(5 * time.Second)
	done = true
	time.Sleep(time.Second)

	// read from primary
	ck := MakeClerk(vshost, "")
	var vals [nkeys]string
	for i := 0; i < nkeys; i++ {
		//values should be in pirmary
		vals[i] = ck.Get(strconv.Itoa(i))
		if vals[i] == "" {
			t.Fatalf("Get(%v) failed from primary", i)
		}
	}

	// kill the primary
	for i := 0; i < nservers; i++ {
		if view1.Primary == sa[i].me {
			sa[i].kill()
			break
		}
	}

	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		//
		view, _ := vck.Get()
		if view.Primary == view1.Backup {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	//ensure corret pimary
	view2, _ := vck.Get()
	if view2.Primary != view1.Backup {
		t.Fatal("wrong Primary")
	}

	// read from old backup
	for i := 0; i < nkeys; i++ {
		//should have same values after primary failure
		z := ck.Get(strconv.Itoa(i))
		if z != vals[i] {
			t.Fatalf("Get(%v) from backup; wanted %v, got %v", i, vals[i], z)
		}
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].kill()
	}

	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

//TEST 5
func TestConcurrentSameUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "csu"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: Concurrent Put()s to the same key; unreliable ...\n")

	const nservers = 2
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
		//notice unreliable flag is set, forces RPC to act unreliably neevr sending back anACK.
		//how does this chamge the behavior?
		sa[i].unreliable = true
	}

	//elect rpimary or back or vs error
	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(viewservice.PingInterval * viewservice.DeadPings)

	done := false

	view1, _ := vck.Get()
	const nclients = 3
	const nkeys = 2
	for xi := 0; xi < nclients; xi++ {
		go func(i int) {
			ck := MakeClerk(vshost, "")
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa(rr.Int() % nkeys)
				v := strconv.Itoa(rr.Int())
				ck.Put(k, v)
			}
		}(xi)
	}

	time.Sleep(5 * time.Second)
	done = true
	time.Sleep(time.Second)

	// read from primary
	ck := MakeClerk(vshost, "")
	var vals [nkeys]string
	for i := 0; i < nkeys; i++ {
		vals[i] = ck.Get(strconv.Itoa(i))
		if vals[i] == "" {
			t.Fatalf("Get(%v) failed from primary", i)
		}
	}

	// kill the primary
	for i := 0; i < nservers; i++ {
		if view1.Primary == sa[i].me {
			sa[i].kill()
			break
		}
	}
	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary == view1.Backup {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	view2, _ := vck.Get()
	if view2.Primary != view1.Backup {
		t.Fatal("wrong Primary")
	}

	// read from old backup
	for i := 0; i < nkeys; i++ {
		z := ck.Get(strconv.Itoa(i))
		if z != vals[i] {
			t.Fatalf("Get(%v) from backup; wanted %v, got %v", i, vals[i], z)
		}
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

//TEST 6
// constant put/get while crashing and restarting servers
func TestRepeatedCrash(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "rc"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: Repeated failures/restarts ...\n")
	//start 3 servers for k/v service p,b and idle
	const nservers = 3
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
	}
	//wait for election of P and B
	for i := 0; i < viewservice.DeadPings; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// wait a bit for primary to initialize backup
	time.Sleep(viewservice.DeadPings * viewservice.PingInterval)
	done := false

	//randomly kill and restart servers
	go func() {
		// kill and restart servers
		rr := rand.New(rand.NewSource(int64(os.Getpid())))
		for done == false {
			i := rr.Int() % nservers
			// fmt.Printf("%v killing %v\n", ts(), 5001+i)
			sa[i].kill()

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)

			sa[i] = StartServer(vshost, port(tag, i+1))

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)
		}
	}()

	//start 2 clients that mke put and get requests, and verify the data they
	//wrote tot the databse is in the database
	const nth = 2
	var cha [nth]chan bool
	for xi := 0; xi < nth; xi++ {
		cha[xi] = make(chan bool)
		go func(i int) {
			ok := false
			defer func() { cha[i] <- ok }()
			ck := MakeClerk(vshost, "")
			data := map[string]string{}
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa((i * 1000000) + (rr.Int() % 10))
				wanted, ok := data[k]
				if ok {
					//check if put succedded
					v := ck.Get(k)
					if v != wanted {
						t.Fatalf("key=%v wanted=%v got=%v", k, wanted, v)
					}
				}
				nv := strconv.Itoa(rr.Int())
				ck.Put(k, nv)
				data[k] = nv
				// if no sleep here, then server tick() threads do not get
				// enough time to Ping the viewserver.
				time.Sleep(10 * time.Millisecond)
			}
			ok = true
		}(xi)
	}

	time.Sleep(20 * time.Second)
	done = true

	fmt.Printf("  ... Put/Gets done ... \n")
	//no client failed
	for i := 0; i < nth; i++ {
		ok := <-cha[i]
		if ok == false {
			t.Fatal("child failed")
		}
	}
	//final put/get succueeded
	ck := MakeClerk(vshost, "")
	ck.Put("aaa", "bbb")
	if v := ck.Get("aaa"); v != "bbb" {
		t.Fatalf("final Put/Get failed")
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

//TEST 7
func TestRepeatedCrashUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "rcu"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: Repeated failures/restarts; unreliable ...\n")

	const nservers = 3
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
		//unreiable flag set, rpc's wont send replys
		sa[i].unreliable = true
	}

	for i := 0; i < viewservice.DeadPings; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// wait a bit for primary to initialize backup
	time.Sleep(viewservice.DeadPings * viewservice.PingInterval)

	done := false

	go func() {
		// kill and restart servers
		rr := rand.New(rand.NewSource(int64(os.Getpid())))
		for done == false {
			i := rr.Int() % nservers
			// fmt.Printf("%v killing %v\n", ts(), 5001+i)
			sa[i].kill()

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)

			sa[i] = StartServer(vshost, port(tag, i+1))

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)
		}
	}()
	//since RPC will be unreiable here, to ensure the vlaue gets there and it is theta value
	//the value of the key must be hashed we can garuntee it is that value
	//we borrow the same methodolgy to that TestRepeatedCrashes uses to test and veriy that data is the ame
	//i.e keepy a local map of the data sent to the databse

	const nth = 2
	var cha [nth]chan bool
	for xi := 0; xi < nth; xi++ {
		cha[xi] = make(chan bool)
		go func(i int) {
			ok := false
			defer func() { cha[i] <- ok }()
			ck := MakeClerk(vshost, "")
			data := map[string]string{}
			// rr := rand.New(rand.NewSource(int64(os.Getpid()+i)))
			k := strconv.Itoa(i)
			data[k] = ""
			n := 0
			for done == false {
				v := strconv.Itoa(n)
				pv := ck.PutHash(k, v)
				if pv != data[k] {
					t.Fatalf("ck.Puthash(%s) returned %v but expected %v at iter %d\n", k, pv, data[k], n)
				}
				h := hash(data[k] + v)
				data[k] = strconv.Itoa(int(h))
				v = ck.Get(k)
				if v != data[k] {
					t.Fatalf("ck.Get(%s) returned %v but expected %v at iter %d\n", k, v, data[k], n)
				}
				// if no sleep here, then server tick() threads do not get
				// enough time to Ping the viewserver.
				time.Sleep(10 * time.Millisecond)
				n++
			}
			ok = true

		}(xi)
	}

	time.Sleep(20 * time.Second)
	done = true

	fmt.Printf("  ... Put/Gets done ... \n")

	for i := 0; i < nth; i++ {
		ok := <-cha[i]
		if ok == false {
			t.Fatal("child failed")
		}
	}

	ck := MakeClerk(vshost, "")
	ck.Put("aaa", "bbb")
	if v := ck.Get("aaa"); v != "bbb" {
		t.Fatalf("final Put/Get failed")
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

func proxy(t *testing.T, port string, delay *int) {
	portx := port + "x"
	os.Remove(portx)
	if os.Rename(port, portx) != nil {
		t.Fatalf("proxy rename failed")
	}
	l, err := net.Listen("unix", port)
	if err != nil {
		t.Fatalf("proxy listen failed: %v", err)
	}
	go func() {
		defer l.Close()
		defer os.Remove(portx)
		defer os.Remove(port)
		for {
			c1, err := l.Accept()
			if err != nil {
				t.Fatalf("proxy accept failed: %v\n", err)
			}
			time.Sleep(time.Duration(*delay) * time.Second)
			c2, err := net.Dial("unix", portx)
			if err != nil {
				t.Fatalf("proxy dial failed: %v\n", err)
			}

			go func() {
				for {
					buf := make([]byte, 1000)
					n, _ := c2.Read(buf)
					if n == 0 {
						break
					}
					n1, _ := c1.Write(buf[0:n])
					if n1 != n {
						break
					}
				}
			}()
			for {
				buf := make([]byte, 1000)
				n, err := c1.Read(buf)
				if err != nil && err != io.EOF {
					t.Fatalf("proxy c1.Read: %v\n", err)
				}
				if n == 0 {
					break
				}
				n1, err1 := c2.Write(buf[0:n])
				if err1 != nil || n1 != n {
					t.Fatalf("proxy c2.Write: %v\n", err1)
				}
			}

			c1.Close()
			c2.Close()
		}
	}()
}

//TEST 8
func TestPartition1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "part1"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	ck1 := MakeClerk(vshost, "")

	fmt.Printf("Test: Old primary does not serve Gets ...\n")
	//make proxy
	vshosta := vshost + "a"
	//creates link between two k/v servers, kind ok like a gaetway.
	os.Link(vshost, vshosta)

	s1 := StartServer(vshosta, port(tag, 1))
	delay := 0
	//proxy is used to dely
	proxy(t, port(tag, 1), &delay)

	deadtime := viewservice.PingInterval * viewservice.DeadPings
	time.Sleep(deadtime * 2)
	if vck.Primary() != s1.me {
		t.Fatal("primary never formed initial view")
	}

	s2 := StartServer(vshost, port(tag, 2))
	time.Sleep(deadtime * 2)
	v1, _ := vck.Get()
	if v1.Primary != s1.me || v1.Backup != s2.me {
		t.Fatal("backup did not join view")
	}

	ck1.Put("a", "1")
	check(ck1, "a", "1")

	os.Remove(vshosta)

	// start a client Get(), but use proxy to delay it long
	// enough that it won't reach s1 until after s1 is no
	// longer the primary.
	//this delay ,imics the idea that the servers are on two different networks and must pass througha
	//a proxy or gateway
	delay = 4 //changing the value of delay
	stale_get := false
	go func() {
		x := ck1.Get("a")
		if x == "1" {
			stale_get = true
		}
	}()

	// now s1 cannot talk to viewserver, so view will change,
	// and s1 won't immediately realize.

	for iter := 0; iter < viewservice.DeadPings*3; iter++ {
		if vck.Primary() == s2.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	if vck.Primary() != s2.me {
		t.Fatalf("primary never changed")
	}

	// wait long enough that s2 is guaranteed to have Pinged
	// the viewservice, and thus that s2 must know about
	// the new view.
	time.Sleep(2 * viewservice.PingInterval)

	// change the value (on s2) so it's no longer "1".
	ck2 := MakeClerk(vshost, "")
	ck2.Put("a", "111")
	check(ck2, "a", "111")

	// wait for the background Get to s1 to be delivered.
	time.Sleep(5 * time.Second)
	if stale_get {
		t.Fatalf("Get to old primary succeeded and produced stale value")
	}

	check(ck2, "a", "111")

	fmt.Printf("  ... Passed\n")

	s1.kill()
	s2.kill()
	vs.Kill()
}

//TEST 9
func TestPartition2(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "part2"
	vshost := port(tag+"v", 1)
	vs := viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	ck1 := MakeClerk(vshost, "")

	vshosta := vshost + "a"
	os.Link(vshost, vshosta)

	s1 := StartServer(vshosta, port(tag, 1))
	delay := 0
	proxy(t, port(tag, 1), &delay)

	fmt.Printf("Test: Partitioned old primary does not complete Gets ...\n")

	deadtime := viewservice.PingInterval * viewservice.DeadPings
	time.Sleep(deadtime * 2)
	if vck.Primary() != s1.me {
		t.Fatal("primary never formed initial view")
	}

	s2 := StartServer(vshost, port(tag, 2))
	time.Sleep(deadtime * 2)
	v1, _ := vck.Get()
	if v1.Primary != s1.me || v1.Backup != s2.me {
		t.Fatal("backup did not join view")
	}

	ck1.Put("a", "1")
	check(ck1, "a", "1")

	os.Remove(vshosta)

	// start a client Get(), but use proxy to delay it long
	// enough that it won't reach s1 until after s1 is no
	// longer the primary.
	delay = 5
	stale_get := false
	go func() {
		x := ck1.Get("a")
		if x == "1" {
			stale_get = true
		}
	}()

	// now s1 cannot talk to viewserver, so view will change.

	for iter := 0; iter < viewservice.DeadPings*3; iter++ {
		if vck.Primary() == s2.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	if vck.Primary() != s2.me {
		t.Fatalf("primary never changed")
	}

	s3 := StartServer(vshost, port(tag, 3))
	for iter := 0; iter < viewservice.DeadPings*3; iter++ {
		v, _ := vck.Get()
		if v.Backup == s3.me && v.Primary == s2.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	v2, _ := vck.Get()
	if v2.Primary != s2.me || v2.Backup != s3.me {
		t.Fatalf("new backup never joined")
	}
	time.Sleep(2 * time.Second)

	ck2 := MakeClerk(vshost, "")
	ck2.Put("a", "2")
	check(ck2, "a", "2")

	s2.kill()

	// wait for delayed get to s1 to complete.
	time.Sleep(6 * time.Second)

	if stale_get == true {
		t.Fatalf("partitioned primary replied to a Get with a stale value")
	}

	check(ck2, "a", "2")

	fmt.Printf("  ... Passed\n")

	s1.kill()
	s2.kill()
	s3.kill()
	vs.Kill()
}
