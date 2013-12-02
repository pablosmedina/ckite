package the.walrus.ckite.states

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Future
import scala.util.Try
import org.slf4j.LoggerFactory
import java.util.Random
import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.Command
import java.util.concurrent.atomic.AtomicReference
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.RLog

/**
 *  •! RePCs from candidates and leaders.
 * •! Convert to candidate if election timeout elapses without
 * either:
 * •! Receiving valid AppendEntries RPC, or
 * •! Granting vote to candidate
 */
case object Follower extends State with Logging {

  override def begin(term: Int)(implicit cluster: Cluster) = ElectionTimeout restart

  override def stop(implicit cluster: Cluster) = ElectionTimeout stop

  override def on(command: Command)(implicit cluster: Cluster) = cluster.forwardToLeader(command)

  override def on(appendEntries: AppendEntries)(implicit cluster: Cluster): AppendEntriesResponse = {
    if (appendEntries.term < cluster.local.term) {
      AppendEntriesResponse(cluster.local.term, false)
    } else {
      ElectionTimeout.restart
      cluster.local.updateTermIfNeeded(appendEntries.term)
      
      if (cluster.updateLeader(appendEntries.leaderId)) {
    	LOG.info(s"Following ${cluster.leader}")
      }

      val success = RLog.tryAppend(appendEntries)
      
      AppendEntriesResponse(cluster.local.term, success)
    }
  }

  override def on(requestVote: RequestVote)(implicit cluster: Cluster): RequestVoteResponse = {
    if (requestVote.term < cluster.local.term) {
      RequestVoteResponse(cluster.local.term, false)
    } else {
      cluster setNoLeader //Some member started an election. Assuming no Leader.
      
      cluster.local.updateTermIfNeeded(requestVote.term)
      val grantVote = checkGrantVotePolicy(requestVote)
      if (grantVote) {
        LOG.debug(s"Granting vote to ${requestVote.memberId} in term ${requestVote.term}")
        cluster.local.votedFor.set(Some(requestVote.memberId))
      } else {
        LOG.debug(s"Rejecting vote to ${requestVote.memberId} in term ${requestVote.term}")
      }
      RequestVoteResponse(cluster.local.term, grantVote)
    }
  }
  
  private def checkGrantVotePolicy(requestVote: RequestVote)(implicit cluster: Cluster) = {
    val votedFor = cluster.local.votedFor.get()
    (!votedFor.isDefined || votedFor.get == requestVote.memberId) && isMuchUpToDate(requestVote)
  }

  private def isMuchUpToDate(requestVote: RequestVote)(implicit cluster: Cluster) = {
    val lastLogEntry = RLog.getLastLogEntry
    lastLogEntry.isEmpty || (requestVote.lastLogTerm >= lastLogEntry.get.term && requestVote.lastLogIndex >= lastLogEntry.get.index)
  }

  private def isCurrentTerm(term: Int)(implicit cluster: Cluster) = term == cluster.local.term

}

case object ElectionTimeout extends Logging {

  val electionTimeoutPool = Executors.newFixedThreadPool(1)
  val timeoutFuture = new AtomicReference[Future[_]]()
  val random = new Random()

  def stop() = {
    if (currentTimeoutFuture() != null) {
      currentTimeoutFuture().cancel(true)
    }
  }

  def restart(implicit cluster: Cluster) = {
    stop
    start
  }

  private def start(implicit cluster: Cluster) = {
    timeoutFuture.set(electionTimeoutPool.submit(new Runnable() {
      override def run() = {
        Try {
          Thread.currentThread().setName("ElectionTimeout")
          cluster updateContextInfo
          val electionTimeout =  randomTimeout
          LOG.trace(s"New timeout is $electionTimeout ms")
          Thread.sleep(electionTimeout)
          LOG.debug("Timeout reached! Time to elect a new Leader")
          cluster.local becomeCandidate (cluster.local.term)
        } recover { case e: Exception => LOG.debug("Election timeout interrupted") }
      }
    }))
  }
  
  private def randomTimeout(implicit cluster: Cluster) = {
    val conf = cluster.configuration
    conf.minElectionTimeout + random.nextInt(conf.maxElectionTimeout - conf.minElectionTimeout)
  }

  private def currentTimeoutFuture() = timeoutFuture.get()
  
}