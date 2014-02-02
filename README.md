CKite [![Build Status](https://api.travis-ci.org/pablosmedina/ckite.png)](https://travis-ci.org/pablosmedina/ckite)
=====

A JVM implementation of the [Raft distributed consensus algorithm](http://raftconsensus.github.io/) written in Scala. CKite is a library to be used by distributed applications needing consensus agreement. It is a work in constant progress. For development & testing purposes it contains an embedded key-value store app demonstrating the algorithm functioning trough simple puts and gets. It will be extracted soon from the CKite library as an example of use.

## Features

* Leader Election
* Log Replication
* Cluster Membership Changes
* Log Compaction
* Finagle based RPC between members
* REST admin console

## Example

#### Create a CKite instance using the builder
```scala
val ckite = CKiteBuilder().withLocalBinding("localhost:9091")
                          .withMembersBindings(Seq("localhost:9092","localhost:9093"))
                          .withMinElectionTimeout(1000).withMaxElectionTimeout(1500) //optional
                          .withHeartbeatsInterval(250) //optional
                          .withDataDir("/home/ckite/data") //dataDir for persistent state (log, terms, snapshots, etc...)
                          .withStateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .build
ckite.start()
```
#### Send a write command
```scala
//this Put command is forwarded to the Leader and applied under Raft rules
ckite.write(Put("key1","value1")) 
```

#### Send a consistent read command
```scala
//consistent read commands are forwarded to the Leader
val value = ckite.read[String](Get("key1")) 
```
#### Add a new Member
```scala
//as write commands, cluster membership changes are forwarded to the Leader
ckite.addMember("someHost:9094")
```

#### Remove a Member
```scala
//as write commands, cluster membership changes are forwarded to the Leader
ckite.removeMember("someHost:9094")
```

#### Issue a local read command
```scala
//alternatively you can read from its local state machine allowing possible stale values
val value = ckite.readLocal[String](Get("key1")) 
```

#### Check leadership
```scala
//if necessary waits for elections to end
ckite.isLeader() 
```
#### Stop ckite
```scala
ckite.stop()
```

## Rest admin console

CKite exposes an admin console showing its status and useful metrics. If the rpc port is 9091 then the admin console is exposed under http://localhost:10091/status by default.

#### Leader

```yaml
{
	cluster: {
		term: 1,
		state: "Leader",
		stateInfo: {
			leaderUptime: "2.minutes+13.seconds+148.milliseconds",
			lastHeartbeatsACKs: {
				localhost:9093: "128.milliseconds",
				localhost:9092: "128.milliseconds",
				localhost:9095: "127.milliseconds",
				localhost:9094: "128.milliseconds"
			}
		}
	},
	log: {
		length: 9,
		commitIndex: 9,
		lastLog: {
			term: 1,
			index: 9,
			command: {
				key: "k1",
				value: "v1"
			}
		}
	}
}
```

#### Follower
```yaml
{
	cluster: {
		term: 1,
		state: "Follower",
		stateInfo: {
			following: "Some(localhost:9091)"
		}
	},
	log: {
		length: 9,
		commitIndex: 9,
		lastLog: {
			term: 1,
			index: 9,
			command: {
				key: "k1",
				value: "v1"
			}
		}
	}
}
```

## Running KVStore example (3 members)

#### Run Member 1
```bash
sbt run -Dport=9091 -Dmembers=localhost:9092,localhost:9093 -DdataDir=/tmp/ckite/member1
```
#### Run Member 2
```bash
sbt run -Dport=9092 -Dmembers=localhost:9091,localhost:9093 -DdataDir=/tmp/ckite/member2
```
#### Run Member 3
```bash
sbt run -Dport=9093 -Dmembers=localhost:9092,localhost:9091 -DdataDir=/tmp/ckite/member3
```
#### Put a key-value on the leader member (take a look at the logs for election result)
```bash
curl http://localhost:10091/put/key1/value1
```
#### Get the value of key1 replicated in member 2 
```bash
curl http://localhost:10092/get/key1
```
#### Retrieve the log on any member to see the replicated log entries
```bash
curl http://localhost:10093/rlog
```
#### Add a new member (localhost:9094) to the Cluster
```bash
curl http://localhost:10091/changecluster/localhost:9091,localhost:9092,localhost:9093,localhost:9094
```
#### Run Member 4
```bash
sbt run -Dport=9094 -Dmembers=localhost:9092,localhost:9091,localhost:9093 -DdataDir=/tmp/ckite/member4
```

## Implementation details

  * Built in Scala.
  * [Twitter Finagle](http://twitter.github.io/finagle/).
  * [Thrift](http://thrift.apache.org/).
  * [Twitter Scrooge](http://twitter.github.io/scrooge/).


## Contributions

Feel free to contribute to CKite!. Any kind of help will be very welcome. We are happy to receive pull requests, issues, discuss implementation details, analyze the raft algorithm and whatever it makes CKite a better library. The following is a list of known pendings to be solved in CKite. You can start from there!

## Pendings/WorkToDo 

  * ~~Leader election~~
  * ~~Log replication~~
  * ~~Cluster Membership changes~~
  * ~~Log persistence & compaction~~
  * Extract the key value store app from CKite
  * Metrics / monitoring
  * Akka?
  * Improve rest api for testing
  * Other things...

