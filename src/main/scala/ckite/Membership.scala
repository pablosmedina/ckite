package ckite

import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input

trait Membership {

  def allMembers: Seq[Member]
  
  def allBindings: Seq[String]
  
  def remoteMembers: Seq[RemoteMember]

  def reachMajority(members: Seq[Member]): Boolean
  
  def reachAnyMajority(members: Seq[Member]): Boolean

  def majority: String

  def majoritiesMap: Map[Seq[Member], Int]
  
  def captureState: MembershipState
  
  def index: Long
  
}

trait MembershipState {
  def recoverIn(cluster: Cluster)
  def getMembershipFor(cluster: Cluster): Membership
}

class SimpleMembershipState(var bindings: List[String],var index:Long) extends MembershipState with KryoSerializable  {
   def write(kryo: Kryo, output: Output) = {
       output.writeString(bindings.mkString(","))
       output.writeLong(index)
   }
   
   def read(kryo: Kryo, input: Input) = {
      bindings = input.readString().split(",").toList
      index = input.readLong()
   }
  
   def recoverIn(cluster: Cluster) = {
      val membership = getMembershipFor(cluster)
      cluster.consensusMembership.set(membership)
   }
   
   def getMembershipFor(cluster: Cluster) = {
      val localOption = if (bindings.contains(cluster.local.id)) Some(cluster.local) else None
      val remoteMembers = (bindings diff Seq(cluster.local.id)).map { 
    	  				binding => cluster.obtainRemoteMember(binding).getOrElse(new RemoteMember(cluster, binding))}.toSeq
      new SimpleConsensus(localOption, remoteMembers, index)
   }
}
 
class JointMembershipState(var oldBindings: MembershipState, var newBindings: MembershipState, var index:Long)  extends MembershipState with KryoSerializable {
     def write(kryo: Kryo, output: Output) = {
      kryo.writeClassAndObject(output, oldBindings)
      kryo.writeClassAndObject(output, newBindings)
      output.writeLong(index)
   }
   
   def read(kryo: Kryo, input: Input) = {
      oldBindings = kryo.readClassAndObject(input).asInstanceOf[MembershipState]
      newBindings = kryo.readClassAndObject(input).asInstanceOf[MembershipState]
      index = input.readLong()
   }
  def recoverIn(cluster: Cluster) = {
    cluster.consensusMembership.set(getMembershipFor(cluster))
  }
  def getMembershipFor(cluster: Cluster): Membership = {
    val oldMembership = oldBindings.getMembershipFor(cluster)
    val newMembership = newBindings.getMembershipFor(cluster)
    new JointConsensus(oldMembership, newMembership,index)
  }
}

class SimpleConsensus(local: Option[LocalMember], members: Seq[RemoteMember],idx:Long) extends Membership {

  val resultingMembers  = (if (local.isDefined) (members :+ local.get) else members).toSet[Member].toList
  val quorum = (resultingMembers.size  / 2) + 1
  
  val innerMajoritiesMap: Map[Seq[Member], Int] = Map(resultingMembers -> quorum)
  
  def allMembers =  resultingMembers
  
  def allBindings = resultingMembers map { member => member.id}
  
  def remoteMembers: Seq[RemoteMember] = members

  def reachMajority(membersRequestingMajority: Seq[Member]): Boolean =  membersRequestingMajority.size >= quorum
  
  def reachAnyMajority(members: Seq[Member]): Boolean = reachMajority(members)

  def majority: String = s"$quorum"

  def majoritiesMap: Map[Seq[Member], Int] = innerMajoritiesMap
  
  def captureState = new SimpleMembershipState(allMembers.map{ _.id},idx)
  
  def index = idx
  
  override def toString(): String = allMembers.toString 

}

object SimpleConsensus {
   def apply(local: Option[LocalMember], members: Seq[RemoteMember],index:Long) = new SimpleConsensus(local, members,index)
}

object EmptyMembership extends SimpleConsensus(None,Seq(),0) {
}

class JointConsensus(oldMembership: Membership, newMembership: Membership, idx:Long) extends Membership {

  def allMembers = (oldMembership.allMembers.toSet ++ newMembership.allMembers.toSet).toSet.toSeq

  def allBindings = allMembers map {member => member.id}
  
  def remoteMembers: Seq[RemoteMember] = (oldMembership.remoteMembers.toSeq ++ newMembership.remoteMembers.toSeq).toSet.toSeq
  
  def reachMajority(members: Seq[Member]): Boolean = oldMembership.reachMajority(members) && newMembership.reachMajority(members)

  def reachAnyMajority(members: Seq[Member]): Boolean = oldMembership.reachMajority(members) || newMembership.reachMajority(members)
  
  def majority: String = s"compound majority of [${oldMembership.majority},${newMembership.majority}]"

  def majoritiesMap: Map[Seq[Member], Int] = oldMembership.majoritiesMap ++: newMembership.majoritiesMap

  def captureState = new JointMembershipState(oldMembership.captureState, newMembership.captureState,idx)
  
  def index:Long = idx
  
  override def toString(): String = {
    s"[Cold=(${oldMembership}), Cnew=(${newMembership})]"
  }
}

object JointConsensus {
  def apply(oldMembership: Membership, newMembership: Membership,index:Long) = new JointConsensus(oldMembership, newMembership,index)  
}