package ckite.states

import ckite.{ Cluster, Member }
import ckite.rpc.{ AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse }
import ckite.stats.{ NonLeaderInfo, StateInfo }

import scala.concurrent.{ Future, Promise }

case object Starter extends State {

  override def begin() = {}

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = Future.successful(AppendEntriesResponse(appendEntries.term, false))

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = Future.successful(RequestVoteResponse(requestVote.term, false))

  override def stepDown(term: Int, leaderId: Option[String]) = {}

  override def info(): StateInfo = NonLeaderInfo("")

  override val term: Int = -1

  override val leaderPromise: Promise[Member] = Promise[Member]()

  override protected def getCluster: Cluster = throw new UnsupportedOperationException()

}
