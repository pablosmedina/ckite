CKite - JVM Raft [![Build Status](https://api.travis-ci.org/pablosmedina/ckite.png)](https://travis-ci.org/pablosmedina/ckite)
=====

## Overview

A __JVM__ implementation of the [Raft distributed consensus algorithm](http://raftconsensus.github.io/) written in Scala. CKite is a `consensus library` with an easy to use API intended to be used by distributed applications needing consensus agreement.

It is designed to be agnostic of both the mechanism used to exchange messages between members `(RPC)` and the medium to store the Log `(Storage)`. CKite has a modular architecture with pluggable `RPC` and `Storage` implementations. Custom RPCs and Storages can be easily implemented and configured to be used by CKite. 

## Status

CKite covers all the major topics of Raft including leader election, log replication, log compaction and cluster membership changes. It currently has two implemented modules:

* ckite-finagle: Finagle based RPC module
* ckite-mapdb: MapDB based Storage module

Checkout the latest __Release 0.2.0__ following the instructions detailed below to start playing with it. 

## Features

* Leader Election
* Log Replication
* Cluster Membership Changes
* Log Compaction
* Twitter Finagle integration
* MapDB integration

## Architecture

* `ckite-core` - The core of the library. It implements the Raft consensus protocol. It can be configured with RPCs and Storages.

* `ckite-finagle` - Twitter Finagle based RPC implementation. It uses a Thrift protocol to exchange Raft messages between members. 

* `ckite-mapdb` - MapDB based storage implementation. MapDB provides concurrent Maps, Sets and Queues backed by disk storage or off-heap-memory. It is a fast and easy to use embedded Java database engine.

Comming soon: ckite-chronicle, ckite-akka.

## Getting started (Scala)

#### SBT settings

The latest release 0.2.0 is in Maven central. Add the following sbt dependency to your project settings:

```scala
libraryDependencies += "io.ckite" %% "ckite-core" % "0.2.0"
```
```scala
libraryDependencies += "io.ckite" %% "ckite-finagle" % "0.2.0"
```
```scala
libraryDependencies += "io.ckite" %% "ckite-mapdb" % "0.2.0"
```

## Getting started (Java)

#### Maven settings

Add the following maven dependency to your pom.xml:

```xml
<dependency>
	<groupId>io.ckite</groupId>
	<artifactId>ckite-core</artifactId>
	<version>0.2.0</version>
</dependency>
```

## Example (See [KVStore](https://github.com/pablosmedina/kvstore))

#### 1) Create a StateMachine
```scala
//KVStore is an in-memory distributed Map allowing Puts and Gets operations
class KVStore extends StateMachine {

  private var map = Map[String, String]()
  private var lastIndex: Long = 0

  //Called when a consensus has been reached for a WriteCommand
  //index associated to the write is provided to implement your own persistent semantics
  //see lastAppliedIndex
  def applyWrite = {
    case (index, Put(key: String, value: String)) => {
      map.put(key, value)
      lastIndex = index
      value
    }
  }

  //called when a read command has been received
  def applyRead = {
    case Get(key) => map.get(key)
  }

  //CKite needs to know the last applied write on log replay to 
  //provide exactly-once semantics
  //If no persistence is needed then state machines can just return zero
  def getLastAppliedIndex: Long = lastIndex

  //called during Log replay on startup and upon installSnapshot requests
  def restoreSnapshot(byteBuffer: ByteBuffer) = {
    map = Serializer.deserialize[Map[String, String]](byteBuffer.array())
  }
  //called when Log compaction is required
  def takeSnapshot(): ByteBuffer = ByteBuffer.wrap(Serializer.serialize(map))

}

//WriteCommands are replicated under Raft rules
case class Put(key: String, value: String) extends WriteCommand[String]

//ReadCommands are not replicated but forwarded to the Leader
case class Get(key: String) extends ReadCommand[Option[String]]
```
#### 2) Create a CKite instance using the builder (minimal)
```scala
val ckite = CKiteBuilder().listenAddress("node1:9091").rpc(FinagleThriftRpc) //Finagle based transport
                          .stateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .bootstrap(true) //bootstraps a new cluster. only needed just the first time for the very first node
                          .build
```

#### 3) Create a CKite instance using the builder (extended)
```scala
val ckite = CKiteBuilder().listenAddress("localhost:9091").rpc(FinagleThriftRpc)
                          .members(Seq("localhost:9092","localhost:9093")) //optional seeds to join the cluster
                          .minElectionTimeout(1000).maxElectionTimeout(1500) //optional
                          .heartbeatsPeriod(250) //optional. period to send heartbeats interval when being Leader
                          .dataDir("/home/ckite/data") //dataDir for persistent state (log, terms, snapshots, etc...)
                          .stateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .sync(false) //disables log sync to disk
                          .flushSize(10) //max batch size when flushing log to disk
                          .build
```
#### 4) Start ckite
```scala
ckite.start()
```

#### 4) Send a write command
```scala
//this Put command is forwarded to the Leader and applied under Raft rules
val writeFuture:Future[String] = ckite.write(Put("key1","value1")) 
```

#### 5) Send a consistent read command
```scala
//consistent read commands are forwarded to the Leader
val readFuture:Future[Option[String]] = ckite.read(Get("key1")) 
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
val ckite = CKiteBuilder().listenAddress("node1:9091").rpc(FinagleThriftRpc)
                          .dataDir("/home/ckite/data") //dataDir for persistent state (log, terms, snapshots, etc...)
                          .stateMachine(new KVStore()) //KVStore is an implementation of the StateMachine trait
                          .bootstrap(true) //bootstraps a new cluster. only needed just the first time for the very first node
                          .build
```

## Implementation details

  * Built in Scala 2.11.7 and JDK 7.
  * [Twitter Finagle](http://twitter.github.io/finagle/).
  * [Thrift](http://thrift.apache.org/).
  * [Twitter Scrooge](http://twitter.github.io/scrooge/).
  * [MapDB](http://www.mapdb.org/)
  * [Kryo](https://github.com/EsotericSoftware/kryo)
  * Chronicle (to be implemented)


## Contributions

Feel free to contribute to CKite!. Any kind of help will be very welcome. We are happy to receive pull requests, issues, discuss implementation details, analyze the raft algorithm and whatever it makes CKite a better library. Checkout the issues. You can start from there!


## Importing the project into IntelliJ IDEA

To generate the necessary IDE config files first run the following command and then open the project as usual:

        sbt gen-idea
        
## Importing the project into Eclipse

To generate the necessary IDE config files first run the following command and then open the project as usual:

        sbt eclipse
