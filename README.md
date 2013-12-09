CKite [![Build Status](https://api.travis-ci.org/pablosmedina/ckite.png)](https://travis-ci.org/pablosmedina/ckite)
=====

A Scala implementation of the [Raft distributed consensus algorithm](http://raftconsensus.github.io/). CKite is a library to be used by distributed applications needing consensus agreement. It is a work in constant progress. 
For development & testing purposes it contains an embedded key-value store app demonstrating the algorithm functioning trough simple puts and gets. It will be extracted soon from the CKite library as an example of use.

## Features

* Leader Election
* Log Replication
* Cluster Membership Changes
* Finagle based RPC between members
* Rest interface

## KeyValue store example (3 members)

#### Run Member 1
```bash
sbt run -Dport=9091 -Dmembers=localhost:9092,localhost:9093
```
#### Run Member 2
```bash
sbt run -Dport=9092 -Dmembers=localhost:9091,localhost:9093
```
#### Run Member 3
```bash
sbt run -Dport=9093 -Dmembers=localhost:9092,localhost:9091
```
#### Put a key-value on the leader member (take a look at the logs for election result)
```bash
curl http://localhost:9091/put/key1/value1
```
#### Get the value of key1 replicated in member 2 
```bash
curl http://localhost:9092/get/key1
```
#### Retrieve the log on any member to see the replicated log entries
```bash
curl http://localhost:9093/rlog
```
#### Add a new member (localhost:9094) to the Cluster
```bash
curl http://localhost:9091/changecluster/localhost:9091,localhost:9092,localhost:9093,localhost:9094
```
#### Run Member 4
```bash
sbt run -Dport=9094 -Dmembers=localhost:9092,localhost:9091,localhost:9093
```

## Implementation details

  * Built in Scala.
  * Twitter Finagle.
  * Thrift.
  * Twitter Scrooge.

## Contributions

Feel free to contribute to CKite!. Any kind of help will be very welcome. We are happy to receive pull requests, issues, discuss implementation details, analyze the raft algorithm and whatever it makes CKite a better library. The following is a list of known pendings to be solved in CKite. You can start from there!

## Pendings/WorkToDo 

  * ~~Leader election~~
  * ~~Log replication~~
  * ~~Cluster Membership changes~~
  * Log persistence & compaction
  * Extract the key value store app from CKite
  * Metrics / monitoring
  * Akka?
  * Improve rest api for testing
  * Other things...

