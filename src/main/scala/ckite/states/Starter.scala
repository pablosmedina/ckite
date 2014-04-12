package ckite.states

import ckite.Cluster
import ckite.rpc.WriteCommand
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.RequestVoteResponse
import ckite.rpc.RequestVoteResponse
import ckite.rpc.EnterJointConsensus
import ckite.rpc.Command

case object Starter extends State {

  override def begin(term: Int) = {}

  override def stop = {}

//  override def on[T](command: Command)(implicit cluster: Cluster) = {}

  override def on(appendEntries: AppendEntries): AppendEntriesResponse = AppendEntriesResponse(appendEntries.term, false)

  override def on(requestVote: RequestVote): RequestVoteResponse = RequestVoteResponse(requestVote.term,false)
  
  override def stepDown(term: Int, leaderId: Option[String]) = { }

  override def info(): StateInfo = NonLeaderInfo("")
  
  override protected def getCluster: Cluster = throw new UnsupportedOperationException()
  
}
