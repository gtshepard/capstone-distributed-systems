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

  #### Map Reduce 
  ##### 1. Overview 
   - Distributed Data Processing (Stateless)
     - Map Reduce is framework for processing large amounts of data over a large cluster of commodity machines (Distributed)
     - Map Reduce parallelizes the work load by taking input say some file, and slpitting it up into multiple pieces and indpendenly processing these inddividual pieces simltanousely on different processes. Each job has excatly the information it needs to complete the job and will always produce the correct results. this is because the functional primitives map and reduce will always produce the same output y for some input x (a key notion in functional programming) another way of saying this is that map and reduce are stateless meaning no information needs to be rememebered about previous inputs for these operations to produce a correct result and because the fucntions do not suffer from side-effects, previous rememberd values changing that could alter the result of the operation. (dig into this more)
     
  ##### 2. Setup and Usage
   - Assuming project cloned to home directory 
        -  ``` export GOPATH=$HOME/mit-6.824-dist-system ```
   - Part 1 Test Cases 
        - cd $HOME/mit-6.824-dist-system/src/main 
        - ``` ./test-wc.sh ```   
   - Part 2 and 3 Test Cases 
     - ``` cd $HOME/mit-6.824-dist-system/src/mapreduce ```
     - ``` go test ```
     
  #### Key Value Store 
  1. Fault Tolerance With State 
  2. Alternative Approaches 
  3. How to Run This Case Study 
  
  #### Case Studies Directory Tree
   - A Directory Tree For Case Studies (note: unimportant files have been omitted for brevity)
   
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
    
### Findings

stateless vs. state 
making a reliable computing system out of unreliable components.
if a system maintains state making the system reliable becomes increasingly complex. 
functional design of map reduce, acrually reduces complexity.
compexity of map reduce vs. paxos becomes much more difficult.
how can we show it is indeed more complex
not all problems can be modeled as stateless
i.e a banking system or keyvalue store 
