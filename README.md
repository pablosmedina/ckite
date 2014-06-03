CKite [![Build Status](https://api.travis-ci.org/pablosmedina/ckite.png)](https://travis-ci.org/pablosmedina/ckite)
=====

## Overview

A __JVM__ implementation of the [Raft distributed consensus algorithm](http://raftconsensus.github.io/) written in Scala. CKite is a library with an easy to use api for both __Java and Scala__ to be used by distributed applications needing consensus agreement. 

## Status

CKite covers all the major topics of Raft including leader election, log replication, log compaction and cluster membership changes. Checkout the latest __Release 0.1.4__ following the instructions detailed below to start playing with it. 
Performance tests will be included soon.


## Features

* Leader Election
* Log Replication
* Cluster Membership Changes
* Log Compaction
* Finagle based RPC between members


## Getting started (Scala)

#### SBT settings

The latest release 0.1.4 is in Maven central. Add the following sbt dependency to your project settings:

```scala
libraryDependencies += "io.ckite" % "ckite" % "0.1.4"
```

## Getting started (Java)

#### Maven settings

Add the following maven dependency to your pom.xml:

```xml
<dependency>
	<groupId>io.ckite</groupId>
	<artifactId>ckite</artifactId>
	<version>0.1.4</version>
</dependency>
```


## Example

#### 1) Create a StateMachine
```scala
//KVStore is a distributed Map accepting Puts and Gets
class KVStore extends StateMachine {

  val map = new ConcurrentHashMap[String, String]()

  //called when a consensus has been reached for a WriteCommand
  //index associated to the write is provided to implement your own persistent semantics
  //see lastAppliedIndex
  def applyWrite = {
    case (index, Put(key: String, value: String)) => {
      map.put(key, value);
      value
    }
  }
  
  //CKite needs to know the last applied write on log replay to 
  //provide exactly-once semantics
  //If no persistence is needed then state machines can just return zero
  def lastAppliedIndex: Long = 0

  //called when a read command has been received
  def applyRead = {
    case Get(key) => {
      map.get(key)
    }
  }

  //called during Log replay on startup and upon installSnapshot requests
  def deserialize(snapshotBytes: ByteBuffer) = {
	//some deserialization mechanism
  }
 
  //called when Log compaction is required
  def serialize: ByteBuffer = {
	//some serialization mechanism
  }

}

//WriteCommands are replicated under Raft rules
case class Put(key: String, value: String) extends WriteCommand

//ReadCommands are not replicated but forwarded to the Leader
case class Get(key: String) extends ReadCommand
```
#### 2) Create a CKite instance using the builder (minimal)
```scala
val ckite = CKiteBuilder().listenAddress("node1:9091")
                          .dataDir("/home/ckite/data") //dataDir for persistent state (log, terms, snapshots, etc...)
                          .stateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .bootstrap(true) //bootstraps a new cluster. only needed just the first time for the very first node
                          .build
```

#### 3) Create a CKite instance using the builder (extended)
```scala
val ckite = CKiteBuilder().listenAddress("localhost:9091")
                          .members(Seq("localhost:9092","localhost:9093")) //optional seeds to join the cluster
                          .minElectionTimeout(1000).maxElectionTimeout(1500) //optional
                          .heartbeatsPeriod(250) //optional. period to send heartbeats interval when being Leader
                          .dataDir("/home/ckite/data") //dataDir for persistent state (log, terms, snapshots, etc...)
                          .stateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .sync(false) //disables log sync to disk
                          .flushSize(10) //max batch size when flushing log to disk
                          .build
```
#### 4) Start a CKite
```scala
ckite.start()
```

#### 4) Send a write command
```scala
//this Put command is forwarded to the Leader and applied under Raft rules
ckite.write(Put("key1","value1")) 
```

#### 5) Send a consistent read command
```scala
//consistent read commands are forwarded to the Leader
val value = ckite.read(Get("key1")) 
```
#### 6) Add a new Member
```scala
//as write commands, cluster membership changes are forwarded to the Leader
ckite.addMember("someHost:9094")
```

#### 7) Remove a Member
```scala
//as write commands, cluster membership changes are forwarded to the Leader
ckite.removeMember("someHost:9094")
```

#### 8) Send a local read command
```scala
//alternatively you can read from its local state machine allowing possible stale values
val value = ckite.readLocal(Get("key1")) 
```

#### 9) Check leadership
```scala
//if necessary waits for elections to end
ckite.isLeader() 
```
#### 10) Stop ckite
```scala
ckite.stop()
```

## How CKite bootstraps

To start a new cluster you have to run the very first node turning on the bootstrap parameter. This will create an initial configuration with just the first node. The next nodes starts by pointing to the existing ones to join the cluster. 
You can bootstrap the first node using the builder, overriding ckite.bootstrap in your application.conf or by starting your application with a system property -Dckite.bootstrap=true. See [KVStore](https://github.com/pablosmedina/kvstore) for more details.

#### bootstrapping the first node using the builder
```scala
val ckite = CKiteBuilder().listenAddress("node1:9091")
                          .dataDir("/home/ckite/data") //dataDir for persistent state (log, terms, snapshots, etc...)
                          .stateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .bootstrap(true) //bootstraps a new cluster. only needed just the first time for the very first node
                          .build
```
## Implementation details

  * Built in Scala.
  * [Twitter Finagle](http://twitter.github.io/finagle/).
  * [Thrift](http://thrift.apache.org/).
  * [Twitter Scrooge](http://twitter.github.io/scrooge/).
  * [MapDB](http://www.mapdb.org/)
  * [Kryo](https://github.com/EsotericSoftware/kryo)


## Contributions

Feel free to contribute to CKite!. Any kind of help will be very welcome. We are happy to receive pull requests, issues, discuss implementation details, analyze the raft algorithm and whatever it makes CKite a better library. Checkout the issues. You can start from there!


## Importing the project into IntelliJ IDEA

To generate the necessary IDE config files first run the following command and then open the project as usual:

        sbt gen-idea
        
## Importing the project into Eclipse

To generate the necessary IDE config files first run the following command and then open the project as usual:

        sbt eclipse
