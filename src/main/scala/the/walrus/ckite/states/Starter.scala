package the.walrus.ckite.states

import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.EnterJointConsensus

case object Starter extends State {

  override def begin(term: Int)(implicit cluster: Cluster) = {}

  override def stop(implicit cluster: Cluster) = {}

  override def on(command: Command)(implicit cluster: Cluster) = {}

  override def on(appendEntries: AppendEntries)(implicit cluster: Cluster): AppendEntriesResponse = AppendEntriesResponse(appendEntries.term, false)

  override def on(requestVote: RequestVote)(implicit cluster: Cluster): RequestVoteResponse = RequestVoteResponse(requestVote.term,false)
  
}
