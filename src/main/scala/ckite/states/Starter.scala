package ckite.states

import ckite.Cluster
import ckite.rpc.WriteCommand
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.RequestVoteResponse
import ckite.rpc.RequestVoteResponse
import ckite.rpc.JointConfiguration
import ckite.rpc.Command
import ckite.stats.StateInfo
import ckite.stats.NonLeaderInfo
import scala.concurrent.Future
import scala.concurrent.Promise
import ckite.Member

case object Starter extends State(-1, Promise[Member]()) {

  override def begin() = {}

  override def on(appendEntries: AppendEntries): Future[AppendEntriesResponse] = Future.successful(AppendEntriesResponse(appendEntries.term, false))

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = Future.successful(RequestVoteResponse(requestVote.term, false))

  override def stepDown(term: Int, leaderId: Option[String]) = {}

  override def info(): StateInfo = NonLeaderInfo("")

  override protected def getCluster: Cluster = throw new UnsupportedOperationException()

}
