package the.walrus.ckite

import java.util.HashMap

trait Membership {

  def allMembers: Seq[Member]
  
  def remoteMembers: Seq[RemoteMember]

  def reachMajority(votes: Seq[Member]): Boolean

  def majority: String

  def majoritiesCount: Int

  def majoritiesMap: Map[Seq[Member], Int]

}

class SimpleMembership(local: Option[LocalMember], members: Seq[RemoteMember]) extends Membership {

  val resultingMembers  = (if (local.isDefined) (members :+ local.get) else members).toSet[Member].toList
  val resultingInternalMajority = ((resultingMembers.size ) / 2) + 1
  
  override def allMembers =  resultingMembers
  
  override def remoteMembers: Seq[RemoteMember] = members

  override def reachMajority(votes: Seq[Member]): Boolean = {
    votes.size >= internalMajority
  }

  def internalMajority = resultingInternalMajority

  override def majority: String = s"${internalMajority}"

  override def majoritiesCount = 1

  override def majoritiesMap: Map[Seq[Member], Int] = {
    Map((resultingMembers, internalMajority))
  }
  
  override def toString(): String = {
    allMembers.map {m => m.id }.mkString(",")
  }

}

class JointConsensusMembership(oldMembership: Membership, newMembership: Membership) extends Membership {

  override def allMembers = (oldMembership.allMembers.toSet ++ newMembership.allMembers.toSet).toSet.toSeq

  override def remoteMembers: Seq[RemoteMember] = (oldMembership.remoteMembers.toSeq ++ newMembership.remoteMembers.toSeq).toSet.toSeq
  
  override def reachMajority(members: Seq[Member]): Boolean = {
    oldMembership.reachMajority(members) && newMembership.reachMajority(members)
  }

  override def majority: String = s"compound majority of [${oldMembership.majority},${newMembership.majority}]"

  override def majoritiesCount = 2

  override def majoritiesMap: Map[Seq[Member], Int] = {
    oldMembership.majoritiesMap ++: newMembership.majoritiesMap
  }

  override def toString(): String = {
    s"[Cold=(${oldMembership}), Cnew=(${newMembership})]"
  }
}