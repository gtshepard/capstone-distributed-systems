## Distributed Systems Research 

### Table of Contents 
1. [Case Studies](#case-studies)
    1. [Map Reduce](#map-reduce)
    2. [View Service](#view-service)
    3. [Primary Backup](#key-value-store)
    4. [Directory Tree](#directory-tree)
    
### Case Studies
  - Assuming project cloned to home directory 
        -  ``` export GOPATH=$HOME/mit-6.824-dist-system ```
  #### Map Reduce
   - Part 1 Test Cases 
        - cd $HOME/mit-6.824-dist-system/src/main 
        - ``` ./test-wc.sh ```   
   - Part 2 and 3 Test Cases 
     - ``` cd $HOME/mit-6.824-dist-system/src/mapreduce ```
     - ``` go test ```
     
  #### View Service 
  
  #### Primary Backup 
    -  go test -run BasicFail
    -  go test -run AtMostOnce 
    -  go test -run FailPut
    -  go test -run ConcurrentSame 
    -  go test -run ConcurrentSameUnreliable 
    -  go test -run Partition1 
    -  go test -run Partition2
    
  do not run go test, becuase it runs all tests. RepeatedCrash and RepeatedCrashUnrebiable do not pass
    
  #### Directory Tree
   - A Directory Tree For Case Studies 
   ```
        mit-6.824-dist-system
        │   README.md
        └───src 
        |   └─── main  
        │   │ test-wc.sh
        |   | kjv12.txt
        |   | wc.go
        │
        └─── mapreduce 
        │   │ common.go
        │   │ mapreduce.go
        |   | master.go
        |   | test_test.go
        |   | worker.go
        |
        └─── viewservice 
        |   | client.go 
        |   | common.go
        |   | server.go
        |   | test_test.go
        |
        └─── pbservice 
            | client.go
            | common.go
            | server.go
            | test_test.go     
      ```
