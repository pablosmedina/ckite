package ckite.states

import ckite.Cluster
import ckite.rpc.Command
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVoteResponse
import ckite.rpc.RequestVote
import ckite.rpc.AppendEntries

case object Stopped extends State {

  override def begin(term: Int) = {}

  override def stop = {}

//  override def on(command: Command)(implicit cluster: Cluster) = {}

  override def on(appendEntries: AppendEntries): AppendEntriesResponse = AppendEntriesResponse(appendEntries.term, false)

  override def on(requestVote: RequestVote): RequestVoteResponse = RequestVoteResponse(requestVote.term,false)
  
  override def canTransition: Boolean = false
  
  override def stepDown(term: Int, leaderId: Option[String]) = { }

  override def info(): StateInfo = NonLeaderInfo("")
  
  override protected def getCluster: Cluster = throw new UnsupportedOperationException()
  
}
