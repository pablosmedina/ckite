package ckite.states

import ckite.{ Cluster, Member }
import ckite.rpc.{ AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse }
import ckite.stats.{ NonLeaderInfo, StateInfo }

import scala.concurrent.{ Future, Promise }

case object Stopped extends State {

  override def begin() = {}

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = Future.successful(AppendEntriesResponse(appendEntries.term, success = false))

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = Future.successful(RequestVoteResponse(requestVote.term, granted = false))

  override def canTransitionTo(state: State): Boolean = false

  override def stepDown(term: Int, leaderId: Option[String]) = {}

  override def info(): StateInfo = NonLeaderInfo("")

  override val term: Int = Int.MaxValue

  override val leaderPromise: Promise[Member] = Promise.failed(new IllegalStateException("Stopped"))

  override protected def getCluster: Cluster = throw new UnsupportedOperationException()

}
