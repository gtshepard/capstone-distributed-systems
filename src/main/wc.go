package main

// import "os"
// import "fmt"
// import "mapreduce/mapreduce"
// import "container/list"

import (
	"container/list"
	"fmt"
	"log"
	"mapreduce"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {

	regRemovePunct, errPunct := regexp.Compile("[^a-zA-Z0-9]+")
	regRemoveStamp, errStamp := regexp.Compile("\\d{3}:\\d{3}")
	regRemoveNumber, errStamp := regexp.Compile("\\d")

	if errPunct != nil {
		log.Fatal(errPunct)
	}

	if errStamp != nil {
		log.Fatal(errStamp)
	}

	//parse text without verse stamp
	noVerseNumber := regRemoveStamp.ReplaceAllString(value, " ")
	text := strings.Split(noVerseNumber, " ")
	keyValueList := list.New()

	for _, element := range text {

		cleanStr := regRemoveNumber.ReplaceAllString(element, "")
		token := regRemovePunct.ReplaceAllString(cleanStr, "")

		mrKeyValue := mapreduce.KeyValue{
			Key:   token,
			Value: "1",
		}

		keyValueList.PushFront(mrKeyValue)
	}

	return keyValueList
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	//aggregate all values for a given key (sum all values for a key)
	count := 0
	for e := values.Front(); e != nil; e = e.Next() {
		count += 1
	}
	frequency := strconv.Itoa(count)
	return frequency
}

func myLogger(step string, msg string, call string, file string) {
	f, err := os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println(step, ": ", msg, "-", call, "-", file)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			//myLogger("0", "before make map", "main()", "wc.go")
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			//	myLogger("1", "after make map", "main()", "wc.go")
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
