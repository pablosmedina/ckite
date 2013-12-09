package the.walrus.ckite

import the.walrus.ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import org.slf4j.MDC
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import java.util.concurrent.Executors
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.LeaveJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.RequestVoteResponse

class Cluster(val configuration: Configuration) extends Logging {

  implicit val aCluster = this
  
  val InitialTerm = 0
  val leader = new AtomicReference[Option[Member]](None)
  val local = new Member(configuration.localBinding)
  
  val consensusMembership = new AtomicReference[Membership]()
  
  def start = {
	updateContextInfo
	LOG.info("Start CKite Cluster")
	consensusMembership.set(new SimpleConsensusMembership(configuration.membersBindings.map(binding => new Member(binding)) :+ local ))
    local becomeFollower InitialTerm
  }

  def on(requestVote: RequestVote) = {
    updateContextInfo
    LOG.debug(s"RequestVote received: $requestVote")
    if (obtainMember(requestVote.memberId).isEmpty) {
      LOG.warn(s"Reject vote to member ${requestVote.memberId} who is not present in the Cluster")
      RequestVoteResponse(local term, false)
    }
    local on requestVote
  }

  def on(appendEntries: AppendEntries) = {
    updateContextInfo
    local on appendEntries
  }

  def on(command: Command) = synchronized {
	updateContextInfo
	LOG.debug(s"Command received: $command")
    local on command
  }
  
  def on(majorityJointConsensus: MajorityJointConsensus) = {
    local on majorityJointConsensus
  }
  
  //Don't broadcast heartbeats during outstanding Command processing
  def broadcastHeartbeats(term: Int)(implicit cluster: Cluster) = synchronized {
    membership.allMembersBut(local).par foreach { member =>
                  Thread.currentThread().setName("Heartbeater")
			      updateContextInfo
			      member.sendHeartbeat(term)(cluster)
    }
  }

  def onReadonly(readonlyCommand: Command) = {
    LOG.debug(s"Readonly command received: $readonlyCommand")
    RLog execute readonlyCommand
  }

  def collectVotes: Seq[Member] = {
    val eventualFollowers = consensusMembership.get.allMembersBut(local).par filter { member => 
	      Thread.currentThread().setName("CollectVotes")
	      updateContextInfo
	      member requestVote 
      }
//    val votes = eventualFollowers.size + 1 //vote for myself
    eventualFollowers.seq :+ local //vote for myself
  }

  def forwardToLeader(command: Command) = {
    if (leader.get().isDefined) {
      leader.get().get.forwardCommand(command)
    } else {
      //should wait for leader?
      LOG.error("No Leader to forward command")
    }
  }

  def updateLeader(memberId: String): Boolean = {
    val newLeader = obtainMember(memberId)
    if (newLeader != leader.get()) {
      leader.set(newLeader)
      updateContextInfo
      true
    }
    else false
  }

  def setNoLeader = {
    leader.set(None)
    updateContextInfo
  }

  def anyLeader =  leader.get() != None

  def majority = membership.majority
  
  def reachMajority(votes: Seq[Member]): Boolean = membership.reachMajority(votes)

  private def obtainMember(memberId: String): Option[Member] = (membership.allMembers).find { _.id == memberId }

  def updateContextInfo = {
    MDC.put("term", local.term.toString)
    MDC.put("leader", leader.get().toString)
  }
  
  def apply(enterJointConsensus: EnterJointConsensus) = {
    LOG.info(s"Entering in JointConsensus")
    val currentMembership = consensusMembership.get()
    consensusMembership.set(new JointConsensusMembership(currentMembership, createSimpleConsensusMembership(enterJointConsensus.newBindings)))
    LOG.info(s"Membership ${consensusMembership.get()}")
  }
  
  def apply(leaveJointConsensus: LeaveJointConsensus) = {
    LOG.info(s"Leaving JointConsensus")
    consensusMembership.set(createSimpleConsensusMembership(leaveJointConsensus.bindings))
    LOG.info(s"Membership ${consensusMembership.get()}")
  }
  
  private def createSimpleConsensusMembership(bindings: Seq[String]): SimpleConsensusMembership = {
    new SimpleConsensusMembership(bindings.map { binding => obtainMember(binding).getOrElse(new Member(binding))})
  }
  
  def membership = consensusMembership.get()
  
  def hasRemoteMembers = !membership.allMembersBut(local).isEmpty
  
}