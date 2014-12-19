package ckite.states

import ckite.Cluster
import ckite.rpc.WriteCommand
import ckite.util.Logging
import ckite.rpc.RequestVoteResponse
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.AppendEntries
import ckite.rpc.JointConfiguration
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.Member
import ckite.RemoteMember
import ckite.stats.StateInfo
import ckite.stats.NonLeaderInfo
import scala.concurrent.Future
import scala.concurrent.Promise
import java.util.concurrent.atomic.AtomicReference

abstract class State(val term: Int, val leaderPromise: Promise[Member], vote: Option[String] = None) extends Logging {

  val votedFor = new AtomicReference[Option[String]](vote)

  def begin() = {
  }

  def stop(term: Int) = {
  }

  def on(requestVote: RequestVote): Future[RequestVoteResponse]

  def on(appendEntries: AppendEntries): Future[AppendEntriesResponse]

  def on[T](command: Command): Future[T] = throw new UnsupportedOperationException()

  def on(jointConsensusCommited: MajorityJointConsensus) = {}

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
      cluster.obtainMember(lid) map { leader ⇒
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

  def info(): StateInfo = NonLeaderInfo(getCluster.leader.toString())

  protected def getCluster: Cluster

  protected def announceLeader(leader: Member) = {
    getCluster.local.currentState.leaderPromise.trySuccess(leader)
  }

  protected def leaderAnnounced = getCluster.local.currentState.leaderPromise.isCompleted

  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {}

}