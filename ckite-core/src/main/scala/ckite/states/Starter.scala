package ckite.states

import ckite.rpc.LogEntry.Term
import ckite.rpc._
import ckite.{ Consensus, LeaderAnnouncer, Membership }

import scala.concurrent.Future

case object Starter extends State {

  override def begin() = {}

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = rejectAppendEntries(appendEntries, "not ready yet")

  override def onRequestVote(requestVote: RequestVote): Future[RequestVoteResponse] = rejectVote(requestVote.memberId, "not ready yet")

  override def stepDown(term: Term) = {}

  override val term: Int = -1

  override protected def membership: Membership = throw new UnsupportedOperationException()

  override protected def consensus: Consensus = throw new UnsupportedOperationException()

  override def leaderAnnouncer: LeaderAnnouncer = throw new UnsupportedOperationException()

  override def onInstallSnapshot(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = rejectInstallSnapshot()

}