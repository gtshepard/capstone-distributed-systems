## Distributed Systems Research 

### Table of Contents 
1. [Abstract](#abstract)
2. [Case Studies](#case-studies)
    1. [Map Reduce](#map-reduce)
      1. [Overview](#overview)
      2. [Setup and Usage](#setup-and-usage)
    2. [Key Value Store](#key-value-store)
      1. [Overview](#overview)
      2. [Setup and Usage](#setup-and-usage)
    3. Directory Tree For Case Studies 
3. [Findings](#findings)

### Abstact

### Case Studies

  #### Map Reduce Lab 1
  ##### Setup and Usage
   - Assuming project cloned to home directory 
        -  ``` export GOPATH=$HOME/mit-6.824-dist-system ```
   - Part 1 Test Cases 
        - cd $HOME/mit-6.824-dist-system/src/main 
        - ``` ./test-wc.sh ```   
   - Part 2 and 3 Test Cases 
     - ``` cd $HOME/mit-6.824-dist-system/src/mapreduce ```
     - ``` go test ```
     
  #### Key Value Store Primary/Backup Lab 2
    - go test -run BasicFail
    - go test -run AtMostOnce
    - go test -run FailPut
    - go test -run ConcurrentSame
    - go test -run ConcurrentSameUnreliable
    - go test -run Partition1
    - go test -run Partition2
    
    - Note: do not run go test, becuase it runs all tests/ RepeatedCrash and RepeatedCrashUnrebiable do not pass
    
  
  #### Directory Tree
   - A Directory Tree For Case Studies 
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
