package the.walrus.ckite

import the.walrus.ckite.example.KVStore
import the.walrus.ckite.example.Put
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import the.walrus.ckite.example.Get

@RunWith(classOf[JUnitRunner])
class CKiteIntegrationTest extends FlatSpec with Matchers {
  
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
  
  
  "A single member cluster" should "read committed writes" in {
     val ckite = CKiteBuilder().withLocalBinding("localhost:9091")
    		 				   .withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
     ckite start
     
     ckite.write(Put(Key1,Value1))
     
     val readValue = ckite.read[String](Get(Key1))
     
     readValue should be (Value1)
     
     ckite stop
  }
  
  "A 3 member cluster" should "elect a Leader" in {
     //member1 has default election timeout (150ms - 300ms). It is intended to be the first to start an election and raise as the leader.
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(4000).withMaxElectionTimeout(5000) //higher election timeout
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(4000).withMaxElectionTimeout(5000) //higher election timeout
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     (members filter {_ isLeader} size) should be (1)
     
     members foreach {_ stop}
  }
  
  "A 3 member cluster" should "failover Leader" in {
     //member1 has default election timeout (150ms - 300ms). It is intended to be the first to start an election and raise as the leader.
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(1000).withMaxElectionTimeout(1000) //1 second election timeout. Should be the leader on failover
    		 				   .withDataDir(someTmpDir)
    		 					.withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(2000) //bigger election timeout. Won't be leader.
    		 				   .withDataDir(someTmpDir)
    		 					.withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     val leaders = members filter {_ isLeader}
     val followers = members filterNot {_ isLeader}
     
     //leader stops
     leaders foreach {_ stop}
     
     //wait for followers election timeout
     Thread.sleep(2000)
     
     //a leader must be elected from the followers
     (followers filter {_ isLeader} size) should be (1)
     
     followers foreach {_ stop}
  }
  
    "A 3 member cluster" should "read committed writes" in {
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 				     .withDataDir(someTmpDir)
    		 					 .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(1000).withMaxElectionTimeout(1000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     member1.write(Put(Key1,Value1))
     
     val member1Read = member1.read[String](Get(Key1))
     val member2Read = member2.read[String](Get(Key1))
     val member3Read = member3.read[String](Get(Key1))
     
     member1Read should be (Value1)
     member2Read should be (Value1)
     member3Read should be (Value1)
     
     members foreach {_ stop}
  }
    
   "A 3 member cluster" should "forward writes to the Leader" in {
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					 .withDataDir(someTmpDir)
    		 				     .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(1000).withMaxElectionTimeout(1000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     member2.write(Put(Key1,Value1))
     
     val member1Read = member1.read[String](Get(Key1))
     val member2Read = member2.read[String](Get(Key1))
     val member3Read = member3.read[String](Get(Key1))
     
     member1Read should be (Value1)
     member2Read should be (Value1)
     member3Read should be (Value1)
     
     members foreach {_ stop}
  }
   
  "A 3 member cluster" should "replicate missing commands on restarted member" in {
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withDataDir(someTmpDir)
    		 				     .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(1000).withMaxElectionTimeout(1000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     //member3 goes down
     member3.stop()
     
     //still having a quorum. This write is committed.
     member1.write(Put(Key1,Value1))
     
     //member3 is back
     val restartedMember3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(2000).withDataDir(someTmpDir)
    		 				   .withStateMachine(new KVStore()).build
     restartedMember3.start()
     
     //wait some time (> heartbeatsInterval) for missing appendEntries to arrive
     Thread.sleep(1000)
     
     //read from its local state machine to check if missing appendEntries have been replicated
     val readValue = restartedMember3.readLocal[String](Get(Key1))
     
     readValue should be (Value1)
     
     member1.stop()
     member2.stop()
     restartedMember3.stop()
  } 
  
  
  private def someTmpDir: String = {
    "/tmp/"+System.currentTimeMillis()
  }
}