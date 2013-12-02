package the.walrus.ckite

import the.walrus.ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import org.slf4j.MDC
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import java.util.concurrent.Executors

class Cluster(val configuration: Configuration) extends Logging {

  implicit val aCluster = this
  
  val InitialTerm = 0
  val leader = new AtomicReference[Option[Member]](None)
  val local = new Member(configuration.localBinding)
  
  val membership = new AtomicReference[Membership]()
  
  def start = {
	updateContextInfo
	LOG.info("Start CKite Cluster")
	membership.set(new StableMembership(configuration.membersBindings))
    local becomeFollower InitialTerm
  }

  def on(requestVote: RequestVote) = {
    updateContextInfo
    LOG.debug(s"RequestVote received: $requestVote")
    local on requestVote
  }

  def on(appendEntries: AppendEntries) = {
    updateContextInfo
    local on appendEntries
  }

  def on(command: Command) = synchronized {
	Thread.currentThread().setName("Command")
	updateContextInfo
	LOG.debug(s"Command received: $command")
    local on command
  }
  
  //Don't broadcast heartbeats during outstanding Command processing
  def broadcastHeartbeats(term: Int)(implicit cluster: Cluster) = synchronized {
    membership.get.allMembers.par foreach { member =>
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
    val eventualFollowers = membership.get.allMembers.par filter { member => 
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

  def majority = membership.get.majority
  
  def reachMajority(votes: Seq[Member]): Boolean = membership.get.reachMajority(votes)

  private def obtainMember(memberId: String): Option[Member] = (membership.get.allMembers :+ local).find { _.id == memberId }

  def updateContextInfo = {
    MDC.put("term", local.term.toString)
    MDC.put("leader", leader.get().toString)
  }

  
}