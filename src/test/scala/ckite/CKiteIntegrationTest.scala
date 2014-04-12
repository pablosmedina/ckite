package ckite

import ckite.example.KVStore
import ckite.example.Put
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import ckite.example.Get
import ckite.example.Put
import ckite.exception.WriteTimeoutException
import ckite.util.Logging

@RunWith(classOf[JUnitRunner])
class CKiteIntegrationTest extends FlatSpec with Matchers with Logging {
  
  val Key1 = "key1"
  val Value1 = "value1"
    
  "A single member cluster" should "elect a Leader" in {
     val ckite = CKiteBuilder().withLocalBinding("localhost:9091")
    		 				   .withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
     ckite start
     
     ckite.isLeader should be 
     
     ckite stop
  }
  
  
  it should "read committed writes" in {
     val ckite = CKiteBuilder().withLocalBinding("localhost:9091")
    		 				   .withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
     ckite start
     
     ckite.write(Put(Key1,Value1))
     
     val readValue = ckite.read[String](Get(Key1))
     
     readValue should be (Value1)
     
     ckite stop
  }
  
  "A 3 member cluster" should "elect a single Leader" in withThreeMemberCluster { members =>
     val leader =  members leader
     val followers = members followers
     
     leader should not be null
     followers.length should be (2)
  }
  
  it should "failover Leader" in withThreeMemberCluster { members =>
     val originalLeader = members leader
     val followers = members followers
     
     originalLeader stop
     
     waitSomeTimeForElection
     
     //a leader must be elected from the followers
     val newLeader = followers leader
     
     newLeader should not be null
     newLeader should not be originalLeader
  }
  
    it should "read committed writes" in withThreeMemberCluster { members =>
    		 				   
     val leader = members leader
     
     leader.write(Put(Key1,Value1))
     
     members foreach { member => 
       			member.read[String](Get(Key1)) should be (Value1) }
     
  }
    
   it should "forward writes to the Leader" in withThreeMemberCluster { members =>
    		 				   
     val someFollower = (members followers) head
     
     //this write is forwarded to the Leader
     someFollower.write(Put(Key1,Value1))
     
     members foreach { member => 
       			member.read[String](Get(Key1)) should be (Value1) }
  }
   
  it should "maintain quorum when 1 member goes down" in withThreeMemberCluster { members =>
    		 				   
     val someFollower = (members followers) head
     
     //a member goes down
     someFollower.stop
     
     val leader = members leader
     
     //leader still have quorum. this write is going to be committed
     leader.write(Put(Key1,Value1))
     
     (members diff Seq(someFollower)) foreach { member => 
       			member.read[String](Get(Key1)) should be (Value1) }
  }

  it should "loose quorum when 2 members goes down" in withThreeMemberCluster { members =>

    val leader = members leader

    //all the followers goes down
    (members followers) foreach { _.stop }

    
    //leader no longer have quorum. this write is going to be rejected
    intercept[WriteTimeoutException] {
    	leader.write(Put(Key1, Value1))
    }
  } 
   
   
  it should "replicate missing commands on restarted member" in {

    val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092", "localhost:9093"))
      .withDataDir(someTmpDir).withStateMachine(new KVStore()).build

    val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091", "localhost:9093"))
      .withMinElectionTimeout(1000).withMaxElectionTimeout(1000).withDataDir(someTmpDir)
      .withStateMachine(new KVStore()).build

    val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092", "localhost:9091"))
      .withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
      .withStateMachine(new KVStore()).build
    
    val members = Seq(member1, member2, member3)
    
    members foreach {_ start}
    		 				   
     val restartedMember3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 .withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
    		 .withStateMachine(new KVStore()).build
     try {
     //member3 goes down
     member3.stop()
     
     //still having a quorum. This write is committed.
     member1.write(Put(Key1,Value1))
     
     //member3 is back
     restartedMember3.start()
     
     //wait some time (> heartbeatsInterval) for missing appendEntries to arrive
     waitSomeTimeForAppendEntries
     
     //read from its local state machine to check if missing appendEntries have been replicated
     val readValue = restartedMember3.readLocal[String](Get(Key1))
     
     readValue should be (Value1)
     } finally {
    	 member1.stop()
    	 member2.stop()
    	 restartedMember3.stop()
     }
  } 
  
  it should "add a new member" in withThreeMemberCluster { members =>
    		 				   
     val leader = members leader
     
     leader.write(Put(Key1,Value1))
     
     //add member4 to the cluster
     leader.addMember("localhost:9094")
     
	 val member4 = CKiteBuilder().withLocalBinding("localhost:9094").withMembersBindings(Seq("localhost:9092","localhost:9091","localhost:9093"))
		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
		 				   .withStateMachine(new KVStore()).build
	 //start member4
     member4.start
     
     //get value for k1. this is going to be forwarded to the Leader.
     val replicatedValue = member4.read[String](Get(Key1))
     replicatedValue should be (Value1)
     
     //wait some time (> heartbeatsInterval) for missing appendEntries to arrive
     waitSomeTimeForAppendEntries
     
     //get value for Key1 from local
     val localValue = member4.readLocal[String](Get(Key1))
     
     localValue should be (replicatedValue)
		 				   
     member4.stop
  }

  it should "overwrite uncommitted entries on an old Leader" in withThreeMemberCluster { members =>

    val leader = members leader

    val followers = (members followers)

    //stop the followers
    followers foreach { _.stop }

    //this two writes will timeout since no majority can be reached 
    for (i <- (1 to 2)) {
      intercept[WriteTimeoutException] {
        leader.write(Put(Key1, Value1))
      }
    }
    //at this point the leader has two uncommitted entries
    
    //leader stops
    leader.stop

    //followers came back
    followers foreach { _.start }
    val livemembers = followers

    waitSomeTimeForElection

    //a new leader is elected
    val newleader = livemembers leader

    //old leader came back
    val oldleader = leader
    oldleader.start

    waitSomeTimeForAppendEntries

    //those two uncommitted entries of the oldleader must be overridden and removed by the new Leader as part of appendEntries
    newleader.read[String](Get(Key1)) should be(null)

  }
  
  implicit def membersSequence(members: Seq[CKite]): CKiteSequence = {
      new CKiteSequence(members)
  }
  
  class CKiteSequence(members: Seq[CKite]) {
    
    def followers = members filterNot {_ isLeader}
    def leader =  {
      val leaders = (members diff followers)
      val theLeader = leaders.head
      withClue("Not unique Leader") { leaders diff Seq(theLeader) should be ('empty) }
      theLeader
    }
    
  }
  
  private def withThreeMemberCluster(test: Seq[CKite] => Any) = {
     //member1 has default election timeout (500ms - 700ms). It is intended to be the first to start an election and raise as the leader.
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(1250).withMaxElectionTimeout(1500) //higher election timeout
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(1750).withMaxElectionTimeout(2000) //higher election timeout
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    val members = Seq(member1, member2, member3)
    LOG.info(s"Starting all the members")
    members.par foreach {_ start}
    waitSomeTimeForElection 
     try {
    	 LOG.info(s"Running test...")
         test (members)
     } finally {
       LOG.info(s"Stopping all the members")
       members foreach {_ stop}
     }
  }
  
  private def waitSomeTimeForElection = Thread.sleep(2000)

  private def waitSomeTimeForAppendEntries = Thread.sleep(1000)
  
  private def someTmpDir: String = {
    "/tmp/"+System.currentTimeMillis()
  }
}