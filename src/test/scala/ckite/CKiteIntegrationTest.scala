package ckite

import java.util.concurrent.TimeoutException

import ckite.example.{ Get, KVStore, Put }
import ckite.storage.MemoryStorage
import ckite.util.Logging
import org.scalatest._

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class CKiteIntegrationTest extends FlatSpec with Matchers with Logging {

  val Key1 = "key1"
  val Value1 = "value1"

  val BOOTSTRAP = true

  val Member1Address = "localhost:9091"
  val Member2Address = "localhost:9092"
  val Member3Address = "localhost:9093"
  val Member4Address = "localhost:9094"

  "A single member cluster" should "elect a Leader" in {
    val ckite = CKiteBuilder().listenAddress(Member1Address)
      .stateMachine(new KVStore()).bootstrap(BOOTSTRAP).storage(MemoryStorage()).rpc(TestRpc).build.asInstanceOf[CKiteClient]
    ckite start

    ckite.isLeader should be

    ckite stop
  }

  it should "read committed writes" in {
    val ckite = CKiteBuilder().listenAddress(Member1Address)
      .stateMachine(new KVStore()).bootstrap(BOOTSTRAP).rpc(TestRpc).build
    ckite start

    await(ckite.write(Put(Key1, Value1)))

    val readValue = await(ckite.read(Get(Key1)))

    readValue should be(Value1)

    ckite stop
  }

  it should "compact a log & reload snapshot" in {
    val ckite = CKiteBuilder().listenAddress(Member1Address)
      .compactionThreshold(5 + 1) //5 writes + 1 NoOp
      .stateMachine(new KVStore()).bootstrap(BOOTSTRAP).rpc(TestRpc).build
    ckite start

    await(ckite.write(Put("key1", "value1")))
    await(ckite.write(Put("key2", "value2")))
    await(ckite.write(Put("key3", "value3")))
    await(ckite.write(Put("key4", "value4")))
    await(ckite.write(Put("key5", "value5")))

    //log should be compacted at this point

    await(ckite.write(Put("key6", "value6")))

    waitSomeTimeForElection

    ckite stop

    val ckiteRestarted = restart(ckite)

    await(ckiteRestarted.read(Get("key1"))) should be("value1")
    await(ckiteRestarted.read(Get("key2"))) should be("value2")
    await(ckiteRestarted.read(Get("key3"))) should be("value3")
    await(ckiteRestarted.read(Get("key4"))) should be("value4")
    await(ckiteRestarted.read(Get("key5"))) should be("value5")

    ckiteRestarted.stop
  }

  it should "restore latest cluster configuration from Log" in {
    val ckite = CKiteBuilder().listenAddress(Member1Address)
      .stateMachine(new KVStore()).bootstrap(BOOTSTRAP).rpc(TestRpc).build
    ckite start

    //It is expected to timeout since Member2 is not up and the configuration must to committed under the new configuration (member1 and member2)
    //TODO: What if two subsequent JointConfiguration ???
    intercept[TimeoutException] {
      await(ckite.addMember(Member2Address))
    }

    ckite stop

    val ckiteRestarted = restart(ckite)

    val members = ckiteRestarted.members

    members should contain(Member2Address)

    ckiteRestarted.stop
  }

  it should "restore latest cluster configuration from Snapshot" in {
    val ckite = CKiteBuilder().listenAddress(Member1Address)
      .compactionThreshold(2 + 1) //1 writes + 1 NoOp
      .stateMachine(new KVStore()).bootstrap(BOOTSTRAP).rpc(TestRpc).build
    ckite start

    //It is expected to timeout since 9092 is not up and the configuration need to committed under the new configuration (9091 and 9092)
    //TODO: What if two subsequent EnterJointConsensus ???
    intercept[TimeoutException] {
      await(ckite.addMember(Member2Address))
    }

    //This will force the Snapshot. Again, it is expected to timeout.
    intercept[TimeoutException] {
      await(ckite.write(Put(Key1, Value1)))
    }

    waitSomeTimeForAppendEntries

    ckite.stop

    val ckiteRestarted = restart(ckite)

    val members = ckiteRestarted.members

    members should contain(Member2Address)

    ckiteRestarted.stop
  }

  "A 3 member cluster" should "elect a single Leader" in withStartedThreeMemberCluster { members ⇒
    val leader = members leader
    val followers = members followers

    leader should not be null
    followers.length should be(2)
  }

  it should "failover Leader" in withStartedThreeMemberCluster { members ⇒
    val originalLeader = members leader
    val followers = members followers

    originalLeader stop

    waitSomeTimeForElection

    //a leader must be elected from the followers
    val newLeader = followers leader

    newLeader should not be null
    newLeader should not be originalLeader
  }

  it should "read committed writes" in withStartedThreeMemberCluster { members ⇒

    val leader = members leader

    await(leader.write(Put(Key1, Value1)))

    members foreach { member ⇒
      await(member.read(Get(Key1))) should be(Value1)
    }

  }

  it should "forward writes to the Leader" in withStartedThreeMemberCluster { members ⇒

    val someFollower = (members followers) head

    //this write is forwarded to the Leader
    await(someFollower.write(Put(Key1, Value1)))

    members foreach { member ⇒
      await(member.read[String](Get(Key1))) should be(Value1)
    }
  }

  it should "maintain quorum when 1 member goes down" in withStartedThreeMemberCluster { members ⇒

    val someFollower = (members followers) head

    //a member goes down
    someFollower.stop

    val leader = members leader

    //leader still have quorum. this write is going to be committed
    await(leader.write(Put(Key1, Value1)))

    (members diff Seq(someFollower)) foreach { member ⇒
      await(member.read(Get(Key1))) should be(Value1)
    }
  }

  it should "loose quorum when 2 members goes down" in withStartedThreeMemberCluster { members ⇒

    val leader = members leader

    //all the followers goes down
    (members followers) foreach {
      _.stop
    }

    //leader no longer have quorum. this write is going to be rejected
    intercept[TimeoutException] {
      await(leader.write(Put(Key1, Value1)))
    }
  }

  it should "forward join on restarted member" in withStartedThreeMemberCluster { members ⇒

    val leader = members leader

    //all the followers goes down
    val follower = members.followers.head

    follower.stop()

    val seeds = Set(Member1Address, Member2Address, Member3Address) - id(leader) - (id(follower))
    builder(follower).storage(MemoryStorage()).members(seeds.toSeq)

    restart(follower)

    waitSomeTimeForAppendEntries()
  }

  it should "replicate missing commands on restarted member" in {

    val member1 = CKiteBuilder().listenAddress(Member1Address)
      .stateMachine(new KVStore()).bootstrap(BOOTSTRAP).rpc(TestRpc).build

    val member2 = CKiteBuilder().listenAddress(Member2Address).members(Seq(Member1Address, Member3Address))
      .minElectionTimeout(1000).maxElectionTimeout(1000)
      .stateMachine(new KVStore()).rpc(TestRpc).build

    val member3 = CKiteBuilder().listenAddress(Member3Address).members(Seq(Member2Address, Member1Address))
      .minElectionTimeout(2000).maxElectionTimeout(2000)
      .stateMachine(new KVStore()).rpc(TestRpc).build

    val members = Seq(member1, member2, member3)

    members foreach {
      _ start
    }

    try {

      val leader = members leader

      //member3 goes down
      member3.stop

      //still having a quorum. This write is committed.
      await(leader.write(Put(Key1, Value1)))

      val seeds = Set(Member1Address, Member2Address, Member3Address) - id(leader) - (id(member3))
      builder(member3).storage(MemoryStorage()).members(seeds.toSeq)

      //member3 is back
      val restartedMember3 = restart(member3)

      //wait some time (> heartbeatsInterval) for missing appendEntries to arrive
      waitSomeTimeForAppendEntries

      //read from its local state machine to check if missing appendEntries have been replicated
      val readValue = restartedMember3.readLocal(Get(Key1))

      readValue should be(Value1)
      restartedMember3.stop
    } finally {
      member1.stop
      member2.stop
    }
  }

  it should "add a new member" in withStartedThreeMemberCluster { members ⇒

    val leader = members leader

    await(leader.write(Put(Key1, Value1)))

    //add member4 to the cluster
    await(leader.addMember(Member4Address))

    val member4 = CKiteBuilder().listenAddress(Member4Address).members(Seq(Member2Address, Member1Address, Member3Address))
      .minElectionTimeout(2000).maxElectionTimeout(3000).stateMachine(new KVStore()).rpc(TestRpc).build.asInstanceOf[CKiteClient]
    //start member4
    member4.start

    //get value for k1. this is going to be forwarded to the Leader.
    val replicatedValue = await(member4.read(Get(Key1)))
    replicatedValue should be(Value1)

    //wait some time (> heartbeatsInterval) for missing appendEntries to arrive
    waitSomeTimeForAppendEntries

    //get value for Key1 from local
    val localValue = member4.readLocal(Get(Key1))

    localValue should be(replicatedValue)

    member4.stop
  }

  it should "overwrite uncommitted entries on an old Leader" in withStartedThreeMemberCluster { members ⇒

    val leader = members leader

    val followers = (members followers)

    //stop the followers
    followers foreach {
      _.stop
    }

    //this two writes will timeout since no majority can be reached 
    for (i ← (1 to 2)) {
      intercept[TimeoutException] {
        await(leader.write(Put(Key1, Value1)))
      }
    }
    //at this point the leader has two uncommitted entries

    //leader stops
    leader.stop

    //followers came back
    val rebuiltFollowers = followers map {
      restart(_)
    }

    val livemembers = rebuiltFollowers

    waitSomeTimeForElection

    //a new leader is elected
    val newleader = livemembers leader

    //old leader came back
    val oldleader = restart(leader)

    waitSomeTimeForAppendEntries

    //those two uncommitted entries of the oldleader must be overridden and removed by the new Leader as part of appendEntries
    await(newleader.read(Get(Key1))) should be(null)

    oldleader.stop
    rebuiltFollowers foreach {
      _.stop
    }

  }

  implicit def membersSequence(members: Seq[CKite]): CKiteSequence = {
    new CKiteSequence(members)
  }

  class CKiteSequence(members: Seq[CKite]) {

    def followers = members filterNot {
      _.asInstanceOf[CKiteClient].isLeader
    }

    def leader = {
      val leaders = (members diff followers)
      val theLeader = leaders.head
      withClue("Not unique Leader") {
        leaders diff Seq(theLeader) should be('empty)
      }
      theLeader
    }

  }

  private def withThreeMemberCluster(block: Seq[CKite] ⇒ Any) = {
    //member1 has default election timeout (500ms - 700ms). It is intended to be the first to start an election and raise as the leader.
    val member1 = CKiteBuilder().listenAddress(Member1Address)
      .bootstrap(true)
      .stateMachine(new KVStore()).rpc(TestRpc).build

    val member2 = CKiteBuilder().listenAddress(Member2Address).members(Seq(Member1Address))
      .minElectionTimeout(1250).maxElectionTimeout(1500) //higher election timeout

      .stateMachine(new KVStore()).rpc(TestRpc).build

    val member3 = CKiteBuilder().listenAddress(Member3Address).members(Seq(Member2Address, Member1Address))
      .minElectionTimeout(1750).maxElectionTimeout(2000) //higher election timeout
      .stateMachine(new KVStore()).rpc(TestRpc).build
    val members = Seq(member1, member2, member3)
    try {
      block(members)
    } finally {
      members.foreach { member ⇒
        try { member.stop() }
      }
    }
  }

  private def withStartedThreeMemberCluster(test: Seq[CKite] ⇒ Any) = withThreeMemberCluster { members ⇒
    logger.info(s"Starting all the members")
    members.foreach(_.start())

    waitSomeTimeForElection
    try {
      logger.info(s"Running test...")
      test(members)
    } finally {
      logger.info(s"Stopping all the members")
      members foreach {
        try { _ stop }
      }
    }
  }

  private def builder(ckite: CKite) = ckite.asInstanceOf[CKiteClient].builder

  private def id(ckite: CKite): String = ckite.asInstanceOf[CKiteClient].id()

  private def restart(ckite: CKite): CKiteClient = {
    val clonedCKite = ckite.asInstanceOf[CKiteClient].builder.stateMachine(new KVStore).bootstrap(false).build.asInstanceOf[CKiteClient]
    clonedCKite.start()
    clonedCKite
  }

  private def waitSomeTimeForElection() = Thread.sleep(3000)

  private def waitSomeTimeForAppendEntries() = Thread.sleep(5000)

  private def await[T](future: Future[T]): T = {
    Await.result(future, 3 seconds)
  }

}