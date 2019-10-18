package viewservice

import (
	"fmt"
	"net/rpc"
)

//
// the viewservice Clerk lives in the client
// and maintains a little state.
//
//the properties of a the basic K/V server
type Clerk struct {
	me     string // client's name (host:port) (the clients identity)
	server string // viewservice's host:port (the master server the view servrivc )
}

//makes a k/v sever which is an object with methodss
func MakeClerk(me string, server string) *Clerk {
	//create new clerk object
	ck := new(Clerk)
	//assign name (identity )to clerk
	ck.me = me
	//assign master (the view server )
	ck.server = server
	//return a k/v server object
	return ck
}

func (ck *Clerk) Failure(srv string) {
	//make RPC call
	args := &ServerFailureArgs{}
	args.Srv = srv
	var reply *ServerFailureReply
	ok := call(ck.server, "ViewServer.HandleServerFailure", args, &reply)

	if ok {
		myLogger("@@@@@@@@@@@@@@", "THIS VS CLERK SPEAKING: "+srv, "", "@@@@@@@@@@@@@@@@@@")
	} else {
		myLogger("@@@@@@@@@@@@@@", "FAILED TO MAKE CALL TO VS: "+srv, "", "@@@@@@@@@@@@@@@@@@")
	}
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

	// in RPC an object can be registered as a server
	// and then all its associated methods are availible as a service.
	// in RPC any object that is registered is considered a server
	// any object that uses one of the servers services (calls a method on the sever) is
	// a client. the client makes requests to the server to get

	//returns a client object that communicates
	//with the server via unix domian sockets.
	// the server is a seperate process
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	//closes client connection to server at the end of this function
	defer c.Close()

	//client makes a request to the server to use the mehtod. then server calls the mehtoed
	err := c.Call(rpcname, args, reply)

	if err == nil {
		//successful call
		return true
	}

	fmt.Println(err)
	return false
}

// rememeber pings do 3 things
//1. inform the view service that the k/v server is still alive
//2. inform the view service of the most recent view that the k/v server knows about
//3. k/v server learns the new view from the view service
func (ck *Clerk) Ping(viewnum uint) (View, error) {

	// prepare the arguments.
	args := &PingArgs{}
	args.Me = ck.me // k/v server name

	//callers notion of the current view number (most recent view k/v sever knows about)
	args.Viewnum = viewnum
	//set up repl variable
	var reply PingReply

	// send an RPC request, wait for the reply.
	ok := call(ck.server, "ViewServer.Ping", args, &reply)

	//call fails
	if ok == false {
		return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
	}

	//reply with reponse from ViewServer.Ping
	return reply.View, nil
}

// gets most recent view information from the view service.
// with out vilunteering to be p/b server.
func (ck *Clerk) Get() (View, bool) {

	//args are empty has no fields
	args := &GetArgs{}
	var reply GetReply

	//make RPC request to ViewServer.Get
	ok := call(ck.server, "ViewServer.Get", args, &reply)

	if ok == false {
		return View{}, false
	}

	//successful call, return reply
	return reply.View, true
}

// retrieves primary for current view from view service, without volunteering to be a p/b serv
func (ck *Clerk) Primary() string {
	//get current view infomation form view service without volunteering to be a p/b server
	v, ok := ck.Get()
	//successfull call
	if ok {
		return v.Primary
	}
	//failed call
	return ""
}
