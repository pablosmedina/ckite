package the.walrus.ckite.states

import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.Member
import the.walrus.ckite.RemoteMember

trait State extends Logging {

  def begin(term: Int) = {}

  def stop = {}

  def on(requestVote: RequestVote): RequestVoteResponse

  def on(appendEntries: AppendEntries): AppendEntriesResponse

  def on[T](command: Command): T = throw new UnsupportedOperationException()
  
  def on(jointConsensusCommited: MajorityJointConsensus) = {}
  
  def canTransition: Boolean = true
  
  /**
   * Step down from being either Candidate or Leader and start following the given Leader
   * on the given Term
   */
  def stepDown(leaderId: Option[String], term: Int) = {
    LOG.debug(s"Step down")
    val cluster = getCluster
	cluster.local.updateTermIfNeeded(term)
    if (leaderId.isDefined) {
//    	cluster.updateLeader(leaderId.get)
    }
    else {
      cluster.setNoLeader
    }
    cluster.local becomeFollower term
  }

  def info(): StateInfo = {
    NonLeaderInfo(getCluster.leader.toString())
  }
  
  protected def getCluster: Cluster
  
  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {}
  
}