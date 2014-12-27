package ckite.states

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import ckite.rpc.{ AppendEntries, AppendEntriesResponse, Command, RequestVote, RequestVoteResponse }
import ckite.util.CKiteConversions._
import ckite.util.Logging
import ckite.{ Cluster, Member }
import com.twitter.concurrent.NamedPoolThreadFactory

import scala.concurrent.{ Future, Promise }

case class Candidate(cluster: Cluster, term: Int, leaderPromise: Promise[Member]) extends State(Some(cluster.local.id)) {

  val election = new Election(cluster)

  override def canTransitionTo(newState: State) = {
    newState match {
      case leader: Leader     ⇒ leader.term == term
      case follower: Follower ⇒ follower.term >= term //in case of split vote or being an old candidate
      case _                  ⇒ newState.term > term
    }
  }

  override def begin() = {
    log.debug(s"Start election")
    election.start(term)
  }

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    if (appendEntries.term < cluster.local.term) {
      rejectAppendEntries(appendEntries, "old term")
    } else {
      log.debug("Leader already elected in term[{}]", appendEntries.term)
      election.abort
      //warn lock
      stepDown(appendEntries.term, Some(appendEntries.leaderId))
      cluster.local.onAppendEntries(appendEntries)
    }
  }

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    if (requestVote.term == term) {
      rejectVote(requestVote.memberId, "being candidate on the same term than remote candidate")
    } else {
      election.abort
      stepDown(requestVote.term, None)
      cluster.local onRequestVote requestVote
    }
  }

  override def on[T](command: Command): Future[T] = {
    cluster.forwardToLeader[T](command)
  }

  override def toString = s"Candidate[$term]"

  override protected def getCluster: Cluster = cluster

}

class Election(cluster: Cluster) extends Logging {

  val executor = new ThreadPoolExecutor(1, 1,
    60L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    new NamedPoolThreadFactory("CandidateElection-worker", true))
  val electionFutureTask = new AtomicReference[java.util.concurrent.Future[_]]()

  def start(inTerm: Int) = {
    val task: Runnable = () ⇒ {
      val votes = cluster collectVotes inTerm

      log.debug(s"Got ${votes.size} votes in a majority of ${cluster.majority}")
      if (cluster.reachMajority(votes)) {
        cluster.local becomeLeader inTerm
      } else {
        log.info(s"Not enough votes to be a Leader")
        cluster.local.becomeFollower(term = inTerm, vote = Some(cluster.local.id)) //voted for my self when Candidate
      }
    }
    electionFutureTask.set(executor.submit(task))
  }

  def abort = {
    log.debug("Abort Election")
    val future = electionFutureTask.get()
    if (future != null) future.cancel(true)
  }

}