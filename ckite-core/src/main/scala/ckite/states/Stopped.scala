package ckite.states

import ckite.rpc.LogEntry._
import ckite.rpc._
import ckite.{ Consensus, LeaderAnnouncer, Membership }

import scala.concurrent.Future

case object Stopped extends State {

  override def begin() = {}

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = Future.successful(AppendEntriesResponse(appendEntries.term, success = false))

  override def onRequestVote(requestVote: RequestVote): Future[RequestVoteResponse] = Future.successful(RequestVoteResponse(requestVote.term, granted = false))

  override def canTransitionTo(state: State): Boolean = false

  override def stepDown(term: Term) = {}

  override val term: Int = Int.MaxValue

  override def isLeader = false

  override def leaderAnnouncer: LeaderAnnouncer = throw new UnsupportedOperationException()

  override protected def membership: Membership = throw new UnsupportedOperationException()

  override protected def consensus: Consensus = throw new UnsupportedOperationException()

  override def onInstallSnapshot(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = rejectInstallSnapshot()

}
