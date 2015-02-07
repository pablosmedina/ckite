package ckite.states

import java.util.concurrent.atomic.AtomicReference

import ckite._
import ckite.rpc.LogEntry.Term
import ckite.rpc._
import ckite.stats.{ NonLeaderInfo, StateInfo }
import ckite.util.Logging

import scala.concurrent.{ Future, Promise }

abstract class State(vote: Option[String] = None) extends Logging {

  val votedFor = new AtomicReference[Option[String]](vote)

  protected val GRANTED, ACCEPTED = true
  protected val REJECTED = false

  def leaderAnnouncer: LeaderAnnouncer
  def term: Term
  protected def membership: Membership
  protected def consensus: Consensus

  def begin() = {
  }

  def stop(term: Term) = {
  }

  def onRequestVote(requestVote: RequestVote): Future[RequestVoteResponse]

  def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse]

  def onCommand[T](command: Command): Future[T] = throw new UnsupportedOperationException()

  def onJointConfigurationCommitted(jointConfiguration: JointConfiguration) = {}

  def onInstallSnapshot(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse]

  def canTransitionTo(newState: State): Boolean = newState.term > term

  protected def stepDown(term: Term): Unit = {
    logger.debug(s"${membership.myId} Step down from being $this")
    consensus.becomeFollower(term = term, leaderAnnouncer = leaderAnnouncer.onStepDown)
  }

  protected def stepDownAndPropagate(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    stepDown(appendEntries.term)
    consensus.onAppendEntries(appendEntries)
  }

  protected def stepDownAndPropagate(requestVote: RequestVote): Future[RequestVoteResponse] = {
    stepDown(requestVote.term)
    consensus.onRequestVote(requestVote)
  }

  protected def rejectVote(candidateRejected: String, reason: String): Future[RequestVoteResponse] = {
    logger.debug(s"Rejecting vote to $candidateRejected due to $reason")
    Future.successful(RequestVoteResponse(term, REJECTED))
  }

  protected def rejectOldCandidate(candidateRejected: String) = {
    rejectVote(candidateRejected, "old Candidate term")
  }

  protected def rejectOldLeader(appendEntries: AppendEntries) = {
    rejectAppendEntries(appendEntries, "old Leader term")
  }

  protected def grantVote() = {
    Future.successful(RequestVoteResponse(term, GRANTED))
  }

  protected def rejectAppendEntries(appendEntries: AppendEntries, reason: String): Future[AppendEntriesResponse] = {
    logger.debug(s"Rejecting $AppendEntries due to $reason")
    Future.successful(AppendEntriesResponse(term, REJECTED))
  }

  protected def rejectInstallSnapshot() = Future.successful(InstallSnapshotResponse(REJECTED))

  def isLeader = {
    leaderAnnouncer.awaitLeader.id().equals(membership.myId)
  }
}