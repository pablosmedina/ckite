package the.walrus.ckite

import the.walrus.ckite.example.KVStore
import the.walrus.ckite.example.Put
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import the.walrus.ckite.example.Get

@RunWith(classOf[JUnitRunner])
class CKiteBuilderTest extends FlatSpec with Matchers {

  it should "elect leader on single member cluster" in {
     val ckite = CKiteBuilder().withLocalBinding("localhost:9091")
    		 				   .withStateMachine(new KVStore()).build
     ckite start
     
     assert(ckite.isLeader)
     
     ckite stop
  }
  
  
  it should "read commited write on single member cluster" in {
     val ckite = CKiteBuilder().withLocalBinding("localhost:9091")
    		 				   .withStateMachine(new KVStore()).build
     ckite start
     
     ckite.write(Put("key1","value1"))
     
     val value = ckite.read[String](Get("key1"))
     
     assertResult("value1")(value)
     
     ckite stop
  }
  
  it should "elect leader on a 3 member cluster" in {
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withMinElectionTimeout(100).withMaxElectionTimeout(200)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(4000).withMaxElectionTimeout(5000)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(4000).withMaxElectionTimeout(5000)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     assertResult(1)(members filter {_ isLeader} size)
     
     members foreach {_ stop}
  }
  
  it should "failover leader on a 3 member cluster" in {
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withMinElectionTimeout(100).withMaxElectionTimeout(200)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(1000).withMaxElectionTimeout(2000)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(2000).withMaxElectionTimeout(3000)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     val leaders = members filter {_ isLeader}
     val followers = members filterNot {_ isLeader}
     
     leaders foreach {_ stop}
     
     while(followers filter {_.isLeader} isEmpty ) {}
     
     assertResult(1)(followers filter {_ isLeader} size)
     
     followers foreach {_ stop}
  }
  
    it should "read commited value on a 3 member cluster" in {
     val member1 = CKiteBuilder().withLocalBinding("localhost:9091").withMembersBindings(Seq("localhost:9092","localhost:9093"))
    		 					.withMinElectionTimeout(100).withMaxElectionTimeout(200)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member2 = CKiteBuilder().withLocalBinding("localhost:9092").withMembersBindings(Seq("localhost:9091","localhost:9093"))
    		 					.withMinElectionTimeout(4000).withMaxElectionTimeout(5000)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val member3 = CKiteBuilder().withLocalBinding("localhost:9093").withMembersBindings(Seq("localhost:9092","localhost:9091"))
    		 					.withMinElectionTimeout(4000).withMaxElectionTimeout(5000)
    		 				   .withStateMachine(new KVStore()).build
    		 				   
     val members = Seq(member1, member2, member3)
     
     members foreach {_ start}
     
     member1.write(Put("key1","value1"))
     
     val member1Read = member1.read[String](Get("key1"))
     val member2Read = member1.read[String](Get("key1"))
     val member3Read = member1.read[String](Get("key1"))
     
     
     assertResult("value1")(member1Read)
     assertResult("value1")(member2Read)
     assertResult("value1")(member3Read)
     
     members foreach {_ stop}
  }
  
  
  
}