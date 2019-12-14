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
3. [Findings](#findings)

### Abstact

### Case Studies
  #### Setup

      
  #### Map Reduce 
  ##### 1. Overview 
   - Distributed Data Processing (Stateless)
  ##### 2. Setup and Usage 
   - Assuming project cloned to home directory 
        - export GOPATH=$HOME/mit-6.824-dist-system
   - Part 1 Test Cases 
        - cd $HOME/mit-6.824-dist-system/src/main 
        - ./test-wc.sh    
   - Part 2 and 3 Test Cases 
     - cd $HOME/mit-6.824-dist-system/src/mapreduce
     - go test 
  
  #### Key Value Store 
  1. Fault Tolerance With State 
  2. Alternative Approaches 
  3. How to Run This Case Study 
    
### Findings

stateless vs. state 
making a reliable computing system out of unreliable components.
if a system maintains state making the system reliable becomes increasingly complex. 
functional design of map reduce, acrually reduces complexity.
compexity of map reduce vs. paxos becomes much more difficult.
how can we show it is indeed more complex
not all problems can be modeled as stateless
i.e a banking system or keyvalue store 


```
project
│   README.md
│   file001.txt    
│
└───folder1
│   │   file011.txt
│   │   file012.txt
│   │
│   └───subfolder1
│       │   file111.txt
│       │   file112.txt
│       │   ...
│   
└───folder2
    │   file021.txt
    │   file022.txt
```
