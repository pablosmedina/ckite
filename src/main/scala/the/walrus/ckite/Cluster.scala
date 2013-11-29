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
  
  val INITIAL_TERM = 0
  val leader = new AtomicReference[Option[Member]](None)
  val local = new Member(configuration.localBinding)
  val members = configuration.membersBindings.map( binding => new Member(binding))
  
  val executor = Executors.newFixedThreadPool(3)
  
  def start() = {
    local becomeFollower (INITIAL_TERM)
    updateContextInfo()
  }

  def onMemberRequestingVote(requestVote: RequestVote) = {
    LOG.debug(s"RequestVote received: $requestVote")
    local.onMemberRequestingVoteReceived(requestVote)
  }

  def onAppendEntriesReceived(appendEntries: AppendEntries): AppendEntriesResponse = {
    local.onAppendEntriesReceived(appendEntries)
  }

  def onCommandReceived(command: Command) = synchronized {
    Thread.currentThread().setName("Command")
    updateContextInfo
    LOG.debug(s"Command received: $command")
    local.onCommandReceived(command)
  }

  def onQueryReceived(query: Command) = {
    LOG.debug(s"Query received: $query")
    RLog.execute(query)
  }

  def collectVotes(): Int = {
    val eventualFollowers = members.par filter { member => 
      Thread.currentThread().setName("CollectVotes")
      updateContextInfo()
      member.requestVote }
    val votes = eventualFollowers.size + 1 //vote for myself
    votes
  }

  def broadcastHeartbeats(term: Int)(implicit cluster: Cluster) = {
    members.foreach { member => 
      executor.submit(new Runnable() {
        override def run() = {
                  Thread.currentThread().setName("Heartbeater")
			      updateContextInfo()
			      member.sendHeartbeat(term)(cluster)
        }
      })
    }
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
      updateContextInfo()
      true
    }
    else false
  }

  def setNoLeader() = {
    leader.set(None)
    updateContextInfo()
  }

  def anyLeader() = {
    leader.get() != None
  }

  def majority = ((members.size + 1) / 2) + 1

  private def obtainMember(memberId: String): Option[Member] = {
    (members :+ local).find { _.id == memberId }
  }

  def updateContextInfo() = {
    MDC.put("term", local.term.toString)
    MDC.put("leader", leader.get().toString)
  }

  
}