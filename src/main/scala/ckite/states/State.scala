package ckite.states

import java.util.concurrent.atomic.AtomicReference

import ckite.{ Cluster, Member, RemoteMember }
import ckite.rpc.{ AppendEntries, AppendEntriesResponse, Command, MajorityJointConsensus, RequestVote, RequestVoteResponse }
import ckite.stats.{ NonLeaderInfo, StateInfo }
import ckite.util.Logging

import scala.concurrent.{ Future, Promise }

abstract class State(vote: Option[String] = None) extends Logging {

  val votedFor = new AtomicReference[Option[String]](vote)

  def term: Int

  def leaderPromise: Promise[Member]

  def begin() = {
  }

  def stop(term: Int) = {
  }

  def on(requestVote: RequestVote): Future[RequestVoteResponse]

  def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse]

  def on[T](command: Command): Future[T] = throw new UnsupportedOperationException()

  def on(jointConsensusCommitted: MajorityJointConsensus) = {}

  def canTransitionTo(newState: State): Boolean = {
    newState.term > term
  }

  /**
   * Step down from being either Candidate or Leader and start following the given Leader
   * on the given Term
   */
  def stepDown(term: Int, leaderId: Option[String]): Unit = {
    val cluster = getCluster
    log.debug(s"${cluster.local.id} Step down from being $this")

    leaderId flatMap { lid: String ⇒
      cluster.membership.obtainMember(lid) map { leader ⇒
        announceLeader(leader)
        cluster.local.becomeFollower(term = term, leaderPromise = Promise.successful(leader))
      }
    } orElse {
      if (!leaderAnnounced) {
        //propagate leader promise
        cluster.local becomeFollower (term = term, leaderPromise = cluster.local.currentState.leaderPromise)
      } else {
        cluster.local becomeFollower term
      }
      None
    }
  }

  def rejectVote(candidateRejected: String, reason: String): Future[RequestVoteResponse] = {
    log.debug(s"Rejecting vote to $candidateRejected due to $reason")
    Future.successful(RequestVoteResponse(term, false))
  }

  def rejectAppendEntries(appendEntries: AppendEntries, reason: String): Future[AppendEntriesResponse] = {
    log.debug(s"Rejecting $AppendEntries due to $reason")
    Future.successful(AppendEntriesResponse(term, false))
  }

  def info(): StateInfo = NonLeaderInfo(getCluster.leader.toString())

  protected def getCluster: Cluster

  protected def announceLeader(leader: Member) = {
    getCluster.local.currentState.leaderPromise.trySuccess(leader)
  }

  protected def leaderAnnounced = getCluster.local.currentState.leaderPromise.isCompleted

  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {}

}