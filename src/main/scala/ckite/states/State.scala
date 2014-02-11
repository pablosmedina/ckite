package ckite.states

import ckite.Cluster
import ckite.rpc.WriteCommand
import ckite.util.Logging
import ckite.rpc.RequestVoteResponse
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.AppendEntries
import ckite.rpc.EnterJointConsensus
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.Member
import ckite.RemoteMember

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
    LOG.debug(s"Step down from being $this")
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