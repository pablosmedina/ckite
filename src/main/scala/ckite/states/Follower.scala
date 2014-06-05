package ckite.states

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledExecutorService
import scala.util.Try
import org.slf4j.LoggerFactory
import java.util.Random
import ckite.Cluster
import ckite.rpc.WriteCommand
import java.util.concurrent.atomic.AtomicReference
import ckite.rpc.RequestVoteResponse
import ckite.util.Logging
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.AppendEntries
import ckite.RLog
import ckite.rpc.JointConfiguration
import java.util.concurrent.atomic.AtomicBoolean
import ckite.rpc.Command
import ckite.util.CKiteConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import ckite.Member
import scala.concurrent.Promise


/**
 *  •! RePCs from candidates and leaders.
 * •! Convert to candidate if election timeout elapses without
 * either:
 * •! Receiving valid AppendEntries RPC, or
 * •! Granting vote to candidate
 */
class Follower(cluster: Cluster, passive: Boolean = false, term: Int, leaderPromise: Promise[Member]) extends State(term, leaderPromise) with Logging {

  val electionTimeout = new ElectionTimeout(cluster, term)
  val votedFor = new AtomicReference[String]("")
  
  override def begin() = {
    if (!passive) electionTimeout restart
  }

  override def stop(stopTerm: Int) = {
    if (stopTerm > term) {
    	electionTimeout stop
    	
    	true
    } else false
  }

  override def on[T](command: Command): Future[T] = cluster.forwardToLeader[T](command)

  override def on(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    if (appendEntries.term < term) {
      Future.successful(AppendEntriesResponse(term, false))
    } else {
      electionTimeout restart

      if (appendEntries.term > term) {
        stepDown(appendEntries.term, Some(appendEntries.leaderId))
        cluster.local on appendEntries
      } else {

        if (!leaderPromise.isCompleted) {
          cluster.obtainMember(appendEntries.leaderId) map { leader =>
            if (leaderPromise.trySuccess(leader)) {
              LOG.info("Following {} in term[{}]", cluster.leader,term)
              cluster.local.persistState
            }
          }
        }
        
        cluster.rlog.tryAppend(appendEntries) map { success =>
          AppendEntriesResponse(cluster.local.term, success)
        }
      }
    }
  }

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    requestVote.term match {
      case requestTerm if requestTerm < term => Future.successful(RequestVoteResponse(term, false))
      case requestTerm if requestTerm > term => {
        stepDown(requestVote.term, None)
        cluster.local on requestVote
      }
      case requestTerm if requestTerm == term => {
        val couldGrantVote = checkGrantVotePolicy(requestVote)
        if (couldGrantVote) {
          if (votedFor.compareAndSet("", requestVote.memberId) || votedFor.get() == requestVote.memberId) {
        	  LOG.debug(s"Granting vote to ${requestVote.memberId} in term ${term}")
        	  cluster.local.votedFor.set(requestVote.memberId)
        	  electionTimeout.restart
        	  cluster.local.persistState()
        	  Future.successful(RequestVoteResponse(term, true))
          } else {
              LOG.debug(s"Rejecting vote to ${requestVote.memberId} in term ${term}. Already voted for ${votedFor.get()}")
              Future.successful(RequestVoteResponse(term, false))
          }
        } else {
          LOG.debug(s"Rejecting vote to ${requestVote.memberId} in term ${term}")
          Future.successful(RequestVoteResponse(term, false))
        }
      }
    }
  }
  
  private def checkGrantVotePolicy(requestVote: RequestVote) = {
    val votedFor = cluster.local.votedFor.get()
    (votedFor.isEmpty() || votedFor == requestVote.memberId) && isMuchUpToDate(requestVote)
  }

  private def isMuchUpToDate(requestVote: RequestVote) = {
    val lastLogEntry = cluster.rlog.getLastLogEntry
    lastLogEntry.isEmpty || (requestVote.lastLogTerm >= lastLogEntry.get.term && requestVote.lastLogIndex >= lastLogEntry.get.index)
  }

  private def isCurrentTerm(term: Int) = term == cluster.local.term
  
  override def toString = s"Follower[$term]"
    
  override protected def getCluster: Cluster = cluster
    
}

class ElectionTimeout(cluster: Cluster, term: Int) extends Logging {

  val scheduledFuture = new AtomicReference[ScheduledFuture[_]]()
  val random = new Random()

  def restart = {
    stop
    start
  }
  
  private def start = {
    val electionTimeout =  randomTimeout
    LOG.trace(s"New timeout is $electionTimeout ms")
    val task: Runnable = () => {
    	  LOG.debug("Timeout reached! Time to elect a new leader")
    	  cluster.local becomeCandidate (term + 1)
          
    }
    val future = cluster.scheduledElectionTimeoutExecutor.schedule(task, electionTimeout, TimeUnit.MILLISECONDS)
    val previousFuture = scheduledFuture.getAndSet(future)
    cancel(previousFuture)
  }

  private def randomTimeout = {
    val conf = cluster.configuration
    val diff = conf.maxElectionTimeout - conf.minElectionTimeout
    conf.minElectionTimeout + random.nextInt(if (diff > 0) diff.toInt else 1)
  }
  
  def stop() = {
    val future = scheduledFuture.get()
    cancel(future)
  }
  
  private def cancel(future: java.util.concurrent.Future[_]) = if (future != null) future.cancel(false)
  
}