package ckite

trait Membership {

  def allMembers: Seq[Member]
  
  def allBindings: Seq[String]
  
  def remoteMembers: Seq[RemoteMember]

  def reachMajority(members: Seq[Member]): Boolean
  
  def reachAnyMajority(members: Seq[Member]): Boolean

  def majority: String

  def majoritiesMap: Map[Seq[Member], Int]
  
  def captureState: MembershipState
  
}

trait MembershipState extends Serializable {
  def recoverIn(cluster: Cluster)
  def getMembershipFor(cluster: Cluster): Membership
}

class SimpleMembershipState(bindings: List[String]) extends MembershipState {
   def recoverIn(cluster: Cluster) = {
      val membership = getMembershipFor(cluster)
      cluster.consensusMembership.set(membership)
   }
   
   def getMembershipFor(cluster: Cluster) = {
      val localOption = if (bindings.contains(cluster.local.id)) Some(cluster.local) else None
      val remoteMembers = (bindings diff Seq(cluster.local.id)).map { 
    	  				binding => cluster.obtainRemoteMember(binding).getOrElse(new RemoteMember(cluster, binding))}.toSeq
      new SimpleConsensus(localOption, remoteMembers)
   }
}
 
class JointMembershipState(oldBindings: MembershipState, newBindings: MembershipState)  extends MembershipState {
  def recoverIn(cluster: Cluster) = {
    cluster.consensusMembership.set(getMembershipFor(cluster))
  }
  def getMembershipFor(cluster: Cluster): Membership = {
    val oldMembership = oldBindings.getMembershipFor(cluster)
    val newMembership = newBindings.getMembershipFor(cluster)
    new JointConsensus(oldMembership, newMembership)
  }
}

class SimpleConsensus(local: Option[LocalMember], members: Seq[RemoteMember]) extends Membership {

  val resultingMembers  = (if (local.isDefined) (members :+ local.get) else members).toSet[Member].toList
  val quorum = (resultingMembers.size  / 2) + 1
  
  def allMembers =  resultingMembers
  
  def allBindings = resultingMembers map { member => member.id}
  
  def remoteMembers: Seq[RemoteMember] = members

  def reachMajority(membersRequestingMajority: Seq[Member]): Boolean =  membersRequestingMajority.size >= quorum
  
  def reachAnyMajority(members: Seq[Member]): Boolean = reachMajority(members)

  def majority: String = s"$quorum"

  def majoritiesMap: Map[Seq[Member], Int] = Map(resultingMembers -> quorum)
  
  def captureState = new SimpleMembershipState(allMembers.map{ _.id})
  
  override def toString(): String = allMembers.toString 

}

object SimpleConsensus {
   def apply(local: Option[LocalMember], members: Seq[RemoteMember]) = new SimpleConsensus(local, members)
}

object EmptyMembership extends SimpleConsensus(None,Seq())

class JointConsensus(oldMembership: Membership, newMembership: Membership) extends Membership {

  def allMembers = (oldMembership.allMembers.toSet ++ newMembership.allMembers.toSet).toSet.toSeq

  def allBindings = allMembers map {member => member.id}
  
  def remoteMembers: Seq[RemoteMember] = (oldMembership.remoteMembers.toSeq ++ newMembership.remoteMembers.toSeq).toSet.toSeq
  
  def reachMajority(members: Seq[Member]): Boolean = oldMembership.reachMajority(members) && newMembership.reachMajority(members)

  def reachAnyMajority(members: Seq[Member]): Boolean = oldMembership.reachMajority(members) || newMembership.reachMajority(members)
  
  def majority: String = s"compound majority of [${oldMembership.majority},${newMembership.majority}]"

  def majoritiesMap: Map[Seq[Member], Int] = oldMembership.majoritiesMap ++: newMembership.majoritiesMap

  def captureState = new JointMembershipState(oldMembership.captureState, newMembership.captureState)
  
  override def toString(): String = {
    s"[Cold=(${oldMembership}), Cnew=(${newMembership})]"
  }
}

object JointConsensus {
  def apply(oldMembership: Membership, newMembership: Membership) = new JointConsensus(oldMembership, newMembership)  
}