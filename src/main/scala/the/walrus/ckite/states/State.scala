package the.walrus.ckite.states

import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries

trait State extends Logging {

  def begin(term: Int)(implicit cluster: Cluster) = {}

  def stop(implicit cluster: Cluster) = {}

  def onRequestVoteReceived(requestVote: RequestVote)(implicit cluster: Cluster): RequestVoteResponse

  def onAppendEntriesReceived(appendEntries: AppendEntries)(implicit cluster: Cluster): AppendEntriesResponse

  def onCommandReceived(command: Command)(implicit cluster: Cluster) = {}
  
  /**
   * Step down from being either Candidate or Leader and start following the given Leader
   * on the given Term
   */
  def stepDown(leaderId: Option[String], term: Int)(implicit cluster: Cluster) = {
	cluster.local.updateTermIfNeeded(term)
    if (leaderId.isDefined) {
    	cluster.updateLeader(leaderId.get)
    }
    else {
      cluster.setNoLeader()
    }
    cluster.local becomeFollower term
  }


}