CKite
=====

A Scala implementation of the Raft distributed consensus algorithm.

## Example (3 members)

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
#### Retrieve the log on the followers to see replicated values
```bash
curl http://localhost:9092/rlog
```

## Implementation details

  * Built in Scala.
  * Twitter Finagle.
  * Thrift.
  * Twitter Scrooge.

## Pendings/WorkToDo 

  * Persistent state
  * Log persistence & compaction
  * Cluster Membership changes
  * Embedded & standalone modes
  * Pluggable state machine (first implementation is a Map supporting puts and gets)
  * Metrics / monitoring
  * Akka?
  * Improve rest api for testing
  * Other things...

