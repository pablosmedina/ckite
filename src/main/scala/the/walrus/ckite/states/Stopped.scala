package the.walrus.ckite.states

import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries

case object Stopped extends State {

  override def begin(term: Int) = {}

  override def stop = {}

//  override def on(command: Command)(implicit cluster: Cluster) = {}

  override def on(appendEntries: AppendEntries): AppendEntriesResponse = AppendEntriesResponse(appendEntries.term, false)

  override def on(requestVote: RequestVote): RequestVoteResponse = RequestVoteResponse(requestVote.term,false)
  
  override def canTransition: Boolean = false
  
  override def stepDown(leaderId: Option[String], term: Int) = { }

  override def info(): StateInfo = NonLeaderInfo("")
  
  override protected def getCluster: Cluster = throw new UnsupportedOperationException()
  
}
