package ckite.states

import java.util.Random
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise

import ckite.Cluster
import ckite.Member
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.Command
import ckite.rpc.RequestVote
import ckite.rpc.RequestVoteResponse
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.Logging

/**
 *  •! RePCs from candidates and leaders.
 * •! Convert to candidate if election timeout elapses without
 * either:
 * •! Receiving valid AppendEntries RPC, or
 * •! Granting vote to candidate
 */
case class Follower(cluster: Cluster, passive: Boolean = false, term: Int, leaderPromise: Promise[Member], vote: Option[String]) extends State(vote) with Logging {

  val electionTimeout = new ElectionTimeout(cluster, term)

  override def begin() = {
    if (!passive) electionTimeout restart

    if (leaderPromise.isCompleted) log.info("Following {} in term[{}]", cluster.leader, term)
  }

  override def stop(stopTerm: Int) = {
    if (stopTerm > term) {
      electionTimeout stop
    }
  }

  override def on[T](command: Command): Future[T] = cluster.forwardToLeader[T](command)

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    if (appendEntries.term < term) {
      Future.successful(AppendEntriesResponse(term, false))
    } else {
      electionTimeout restart

      if (appendEntries.term > term) {
        stepDown(appendEntries.term, Some(appendEntries.leaderId))
        cluster.local onAppendEntries appendEntries
      } else {

        if (!leaderPromise.isCompleted) {
          cluster.membership.obtainMember(appendEntries.leaderId) map { leader ⇒
            if (leaderPromise.trySuccess(leader)) {
              log.info("Following {} in term[{}]", cluster.leader, term)
              cluster.local.persistState
            }
          }
        }

        cluster.rlog.tryAppend(appendEntries) map { success ⇒
          AppendEntriesResponse(cluster.local.term, success)
        }
      }
    }
  }

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    requestVote.term match {
      case requestTerm if requestTerm < term ⇒ rejectVote(requestVote.memberId, "old term")
      case requestTerm if requestTerm > term ⇒ {
        stepDown(requestVote.term, None)
        cluster.local onRequestVote requestVote
      }
      case requestTerm if requestTerm == term ⇒ {
        val couldGrantVote = checkGrantVotePolicy(requestVote)
        if (couldGrantVote) {
          if (votedFor.compareAndSet(None, Some(requestVote.memberId)) || votedFor.get().equals(Some(requestVote.memberId))) {
            log.debug(s"Granting vote to ${requestVote.memberId} in term[${term}]")
            electionTimeout.restart
            cluster.local.persistState()
            Future.successful(RequestVoteResponse(term, true))
          } else {
            rejectVote(requestVote.memberId, s"already voted for ${votedFor.get()}")
          }
        } else {
          rejectVote(requestVote.memberId, s"not granted vote policy")
        }
      }
    }
  }

  private def checkGrantVotePolicy(requestVote: RequestVote) = {
    val vote = votedFor.get()
    (!vote.isDefined || vote.get == requestVote.memberId) && isMuchUpToDate(requestVote)
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
    val electionTimeout = randomTimeout
    log.trace(s"New timeout is $electionTimeout ms")
    val task: Runnable = () ⇒ {
      log.debug("Timeout reached! Time to elect a new leader")
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