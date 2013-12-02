package the.walrus.ckite

import java.util.HashMap

trait Membership {

  def allMembers:Seq[Member]
  
  def reachMajority(votes: Seq[Member]): Boolean

  def majority: String
  
  def majoritiesCount: Int
  
  def majoritiesMap: java.util.Map[Seq[Member], Int]
  
}

class StableMembership(bindings: Seq[String]) extends Membership {
  
  val members = bindings.map( binding => new Member(binding) )
  
  override def allMembers = members
  
  override def reachMajority(votes: Seq[Member]): Boolean = {
    votes.size >= internalMajority
  }
  
  def internalMajority = ((members.size + 1) / 2) + 1
  
  override def majority: String = s"simple majority of ${internalMajority}"
  
  override def majoritiesCount = 1
  
  override def majoritiesMap: java.util.Map[Seq[Member], Int] = {
    val map = new HashMap[Seq[Member], Int]()
    map.put(members, (internalMajority - 1))
   map
  }
  
}