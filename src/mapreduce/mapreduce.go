package mapreduce

import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// import "os/exec"

// A simple mapreduce library with a sequential implementation.
//
// The application provides an input file f, a Map and Reduce function,
// and the number of nMap and nReduce tasks.
//
// Split() splits the file f in nMap input files:
//    f-0, f-1, ..., f-<nMap-1>
// one for each Map job.
//
// DoMap() runs Map on each map file and produces nReduce files for a
// map file.  Thus, there will be nMap x nReduce files after all map
// jobs are done:
//    f-0-0, ..., f-0-0, f-0-<nReduce-1>, ...,
//    f-<nMap-1>-0, ... f-<nMap-1>-<nReduce-1>.
//
// DoReduce() collects <nReduce> reduce files from each map (f-*-<reduce>),
// and runs Reduce on those files.  This produces <nReduce> result files,
// which Merge() merges into a single output.

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Map and Reduce deal with <key, value> pairs:
type KeyValue struct {
	Key   string
	Value string
}

type MapReduce struct {
	nMap            int          // Number of Map jobs
	nReduce         int          // Number of Reduce jobs
	file            string       // Name of input file
	MasterAddress   string       //localhost:7777
	registerChannel chan string  //Read start registartion server later
	DoneChannel     chan bool    // channel to signal process is done running
	alive           bool         //denote wether MR job is running or not
	l               net.Listener //IDK
	stats           *list.List   //IDK

	// Map of registered workers that you need to keep up to date
	Workers map[string]*WorkerInfo

	// add any additional state here
}

func InitMapReduce(nmap int, nreduce int,
	file string, master string) *MapReduce {
	myLogger("4", "begin", "InitMapReduce()", "mapreduce.go")
	mr := new(MapReduce)
	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = file
	mr.MasterAddress = master
	mr.alive = true
	mr.registerChannel = make(chan string, 2) //unbuffered channel sender blocks until value recieved
	mr.DoneChannel = make(chan bool)          //unbuffered channel sender blocks until value recieved

	// initialize any additional state here
	mr.Workers = make(map[string]*WorkerInfo)
	return mr
}

func MakeMapReduce(nmap int, nreduce int,
	file string, master string) *MapReduce {
	myLogger("3", "begin", "MakeMapReduce()", "mapreduce.go")
	//initializes map reduce object
	mr := InitMapReduce(nmap, nreduce, file, master)
	myLogger("3", "after initMapReduce", "MakeMapReduce()", "mapreduce.go")
	//registers master so its methods are availbe as a service
	mr.StartRegistrationServer()
	//concurrently executes Run()
	go mr.Run()
	return mr
}

func (mr *MapReduce) Register(args *RegisterArgs, res *RegisterReply) error {
	DPrintf("Register: worker %s\n", args.Worker)
	mr.registerChannel <- args.Worker
	res.OK = true
	return nil
}

func (mr *MapReduce) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown: registration server\n")
	//no nee incoming requests
	mr.alive = false
	mr.l.Close() // causes the Accept to fail
	return nil
}

func (mr *MapReduce) StartRegistrationServer() {
	myLogger("5", "begin", "StartRegistrationServer()", "mapreduce.go")
	//creates a new server for RPC the callee is the client, the reciever is the server
	rpcs := rpc.NewServer()
	//reigster the MR object so its methods are availible for RPC
	rpcs.Register(mr)
	os.Remove(mr.MasterAddress) // only needed for "unix"
	//create a listenr for localhost:7777 (MR  master address)
	l, e := net.Listen("unix", mr.MasterAddress)

	if e != nil {
		log.Fatal("RegstrationServer", mr.MasterAddress, " error: ", e)
	}
	mr.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	// concurently listens for incoming requests
	go func() {
		for mr.alive {
			//blocks until connection recieved, accepts wjhen there is one
			conn, err := mr.l.Accept()
			if err == nil {
				//each time we recieve a connection, we start another thread to handle the request
				go func() {
					//servers client unitl they hang up (how do clients hang up?)
					//in our case client hangs up after the mehtod does its work (test this)
					rpcs.ServeConn(conn)
					//closes connection when done
					conn.Close()
				}()
			} else {
				DPrintf("RegistrationServer: accept error")
				break
			}
		}
		DPrintf("RegistrationServer: done\n")
	}()

	// every object that acts as a server in RPC must be registered to make its methods availible as a service to other entities.
	// the callee (caller of the call function) is the client, which makes a request to the server.
	// we do this for both master and workers because at we need workers to act as both clients & servers and same goes for master
	// meaning sometimes the worker is going to need the services master provides and vice versa kkind of P2P
}

// Name of the file that is the input for map job <MapJob>
func MapName(fileName string, MapJob int) string {
	return "mrtmp." + fileName + "-" + strconv.Itoa(MapJob)
}

// Split bytes of input file into nMap splits, but split only on white space
func (mr *MapReduce) Split(fileName string) {

	fmt.Printf("Split %s\n", fileName)
	//open file
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Split: ", err)
	}
	//close file when Split function ends
	defer infile.Close()

	//return a struct with file information
	fi, err := infile.Stat()

	if err != nil {
		log.Fatal("Split: ", err)
	}
	//get size of file in bytes
	size := fi.Size()

	//define chunk size and make it a integer
	nchunk := size / int64(mr.nMap)
	nchunk += 1

	//handle first file
	//creates a file for a map
	outfile, err := os.Create(MapName(fileName, 0))

	if err != nil {
		log.Fatal("Split: ", err)
	}
	//write to output file
	writer := bufio.NewWriter(outfile)
	m := 1
	i := 0

	//scan input file line by line
	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		//if end of chunk make new output file
		if int64(i) > nchunk*int64(m) {
			writer.Flush()
			outfile.Close()
			outfile, err = os.Create(MapName(fileName, m))
			writer = bufio.NewWriter(outfile)
			m += 1
		}

		//read line and write to output file n
		line := scanner.Text() + "\n"
		writer.WriteString(line)
		//move to next chunk (each characeter is one byte)
		i += len(line)
	}
	//remove buffered  data from bufio.writer
	writer.Flush()
	outfile.Close()
}

//name a reduce file
func ReduceName(fileName string, MapJob int, ReduceJob int) string {
	return MapName(fileName, MapJob) + "-" + strconv.Itoa(ReduceJob)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Read split for job, call Map for that split, and create nreduce
// partitions.
func DoMap(JobNumber int, fileName string,
	nreduce int, Map func(string) *list.List) {

	//gets map name
	name := MapName(fileName, JobNumber)
	//opens file
	file, err := os.Open(name)

	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	//get file info
	fi, err := file.Stat()
	if err != nil {
		log.Fatal("DoMap: ", err)
	}

	size := fi.Size()
	fmt.Printf("DoMap: read split %s %d\n", name, size)
	//make a slice b that has slot for each byte in the file
	b := make([]byte, size)
	//read N bytes into slice b from file
	_, err = file.Read(b)
	if err != nil {
		log.Fatal("DoMap: ", err)
	}

	file.Close()
	//run user defined map function on this string (contents of file with map name)
	//returns a *list.List of type KeyValue (defined above in MR)
	res := Map(string(b))

	// XXX a bit inefficient. could open r files and run over list once
	for r := 0; r < nreduce; r++ {
		//create a reduce file
		file, err = os.Create(ReduceName(fileName, JobNumber, r))
		//check for error
		if err != nil {
			log.Fatal("DoMap: create ", err)
		}
		//files format is now json
		enc := json.NewEncoder(file)
		//traverse list
		for e := res.Front(); e != nil; e = e.Next() {
			//get list value (which is a  KeyValue)
			kv := e.Value.(KeyValue)
			//IDK what this chekc means
			if hash(kv.Key)%uint32(nreduce) == uint32(r) {
				//encode keyvalue pair to json and write to file
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("DoMap: marshall ", err)
				}
			}
		}
		file.Close()
	}
}

func MergeName(fileName string, ReduceJob int) string {
	return "mrtmp." + fileName + "-res-" + strconv.Itoa(ReduceJob)
}

// Read map outputs for partition job, sort them by key, call reduce for each
// key
func DoReduce(job int, fileName string, nmap int,
	Reduce func(string, *list.List) string) {
	//make a lof map where key is string and value is *list.List
	kvs := make(map[string]*list.List)
	for i := 0; i < nmap; i++ {
		//open specified reduce file
		name := ReduceName(fileName, i, job)
		fmt.Printf("DoReduce: read %s\n", name)
		file, err := os.Open(name)

		if err != nil {
			log.Fatal("DoReduce: ", err)
		}
		//decode JSON file
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			//decode fills kv with decode data
			err = dec.Decode(&kv)

			//when no more to decode
			if err != nil {
				break
			}
			//check if map contians key
			_, ok := kvs[kv.Key]

			if !ok {
				//create key and empty list
				kvs[kv.Key] = list.New()
			}
			//if key exists add value to list
			kvs[kv.Key].PushBack(kv.Value)
			//we now have all the values associated with a particular key, so we can now reduce them
		}
		file.Close()
	}
	//create a slice of strings
	var keys []string
	//for key in the map, append the key to the slice
	for k := range kvs {
		keys = append(keys, k)
	}
	//sort the slice of keys
	sort.Strings(keys)

	//create a merge file
	p := MergeName(fileName, job)
	file, err := os.Create(p)

	if err != nil {
		log.Fatal("DoReduce: create ", err)
	}

	//encode the file as Json
	enc := json.NewEncoder(file)
	//for each key in the slice
	for _, k := range keys {
		//call user defined reduce by passing in the key and its list of associated values
		//returns key value pair, where k is the key, v is single reduced value
		res := Reduce(k, kvs[k])
		//JSON encode the pair and write to merge file
		enc.Encode(KeyValue{k, res})
	}
	file.Close()
}

// Merge the results of the reduce jobs
// XXX use merge sort
func (mr *MapReduce) Merge() {
	DPrintf("Merge phase")
	//map where key = string, value = string
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		//open specified merge fil
		p := MergeName(mr.file, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)

		if err != nil {
			log.Fatal("Merge: ", err)
		}

		//decode json
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			//decode a line in the file and store it in kv
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			//add key and value to map
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}

	var keys []string
	//append each key in the map to the slice
	for k := range kvs {
		keys = append(keys, k)
	}
	//sort all the keys in the slice
	sort.Strings(keys)

	//create a file
	file, err := os.Create("mrtmp." + mr.file)

	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	//for each key in the map, write its value to the file
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	//clean the writers buffer and close the file
	w.Flush()
	file.Close()
}

func RemoveFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

func (mr *MapReduce) CleanupFiles() {
	for i := 0; i < mr.nMap; i++ {
		RemoveFile(MapName(mr.file, i))
		for j := 0; j < mr.nReduce; j++ {
			RemoveFile(ReduceName(mr.file, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		RemoveFile(MergeName(mr.file, i))
	}
	RemoveFile("mrtmp." + mr.file)
}

// Run jobs sequentially.
// nMap int: # of map jobs to run
// nReduce int: # of reduce jobs to run
// file string: the inpit file to split,
// Map func(string) *list.List: a user defined function to perform mapping
// Reduce func(string, *list.List) string: a user defined function to perform the reducing
func RunSingle(nMap int, nReduce int, file string,
	Map func(string) *list.List,
	Reduce func(string, *list.List) string) {

	// initializes MapReduce Object and returns an instance
	// pass in empty string becuase no concept of master/worker in sequential version.
	// note of parallel veriosn this call is ade in mapreduce.MakeMapReduce passes in master address
	mr := InitMapReduce(nMap, nReduce, file, "")

	//split file into nMap pieces
	mr.Split(mr.file)

	//run  nMap map jobs
	for i := 0; i < nMap; i++ {
		//perform map
		DoMap(i, mr.file, mr.nReduce, Map)
	}
	// then run nReduce jobs
	for i := 0; i < mr.nReduce; i++ {
		//perfom reduce
		DoReduce(i, mr.file, mr.nMap, Reduce)
	}
	// merge nReduce pieces
	mr.Merge()
	// end of sequential map reduce
}

func (mr *MapReduce) CleanupRegistration() {
	args := &ShutdownArgs{}
	var reply ShutdownReply
	ok := call(mr.MasterAddress, "MapReduce.Shutdown", args, &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.MasterAddress)
	}
	DPrintf("CleanupRegistration: done\n")
}

// Run jobs in parallel, assuming a shared file system
func (mr *MapReduce) Run() {
	myLogger("6", "begin", "Run()", "mapreduce.go")
	fmt.Printf("Run mapreduce job %s %s\n", mr.MasterAddress, mr.file)
	//breaks file into nMap pieces
	mr.Split(mr.file)
	//retuns list ok killed workers
	mr.stats = mr.RunMaster()
	//merge files
	mr.Merge()
	mr.CleanupRegistration()

	fmt.Printf("%s: MapReduce done\n", mr.MasterAddress)

	mr.DoneChannel <- true
}
