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
  val members = configuration.membersBindings.map( binding => new Member(binding) )
  
  def start = {
	updateContextInfo
	LOG.info("Start CKite Cluster")
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
	LOG.debug(s"Command received: $command")
    Thread.currentThread().setName("Command")
    updateContextInfo
    local on command
  }
  
  //Don't broadcast heartbeats during outstanding Command processing
  def broadcastHeartbeats(term: Int)(implicit cluster: Cluster) = synchronized {
    members.par foreach { member =>
                  Thread.currentThread().setName("Heartbeater")
			      updateContextInfo
			      member.sendHeartbeat(term)(cluster)
    }
  }

  def onReadonly(readonlyCommand: Command) = {
    LOG.debug(s"Readonly command received: $readonlyCommand")
    RLog execute readonlyCommand
  }

  def collectVotes = {
    val eventualFollowers = members.par filter { member => 
	      Thread.currentThread().setName("CollectVotes")
	      updateContextInfo
	      member requestVote 
      }
    val votes = eventualFollowers.size + 1 //vote for myself
    votes
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

  def majority = ((members.size + 1) / 2) + 1

  private def obtainMember(memberId: String): Option[Member] = (members :+ local).find { _.id == memberId }

  def updateContextInfo = {
    MDC.put("term", local.term.toString)
    MDC.put("leader", leader.get().toString)
  }

  
}