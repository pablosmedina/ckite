package ckite.states

import ckite.Cluster
import ckite.rpc.WriteCommand
import ckite.util.Logging
import ckite.rpc.RequestVoteResponse
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.AppendEntries
import ckite.rpc.JointConfiguration
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.Member
import ckite.RemoteMember
import ckite.stats.StateInfo
import ckite.stats.NonLeaderInfo
import scala.concurrent.Future
import scala.concurrent.Promise

abstract class State(val term: Int, val leaderPromise: Promise[Member]) extends Logging {

  def begin() = {
  }
  
  def stop(term: Int):Boolean = {
    true
  }

  def on(requestVote: RequestVote): Future[RequestVoteResponse]

  def on(appendEntries: AppendEntries): Future[AppendEntriesResponse]

  def on[T](command: Command): Future[T] = throw new UnsupportedOperationException()
  
  def on(jointConsensusCommited: MajorityJointConsensus) = {}
  
  def canTransitionTo(newState: State): Boolean = {
     newState.term > term
  }
  
  /**
   * Step down from being either Candidate or Leader and start following the given Leader
   * on the given Term
   */
  def stepDown(term: Int, leaderId: Option[String]):Unit = {
    val cluster = getCluster
    LOG.debug(s"${cluster.local.id} Step down from being $this")
    
    val e = leaderId map { lid:String => 
    	cluster.obtainMember(lid) map { leader => 
    	  cluster.local.currentState.leaderPromise.trySuccess(leader)
    	  cluster.local.becomeFollower(term = term, leaderPromise = Promise.successful(leader))
    	
    	}
    } flatten 
    
    e.orElse {
        if (!cluster.local.currentState.leaderPromise.isCompleted) {
        	cluster.local becomeFollower(term = term, leaderPromise = cluster.local.currentState.leaderPromise)
          
        } else {
          cluster.local becomeFollower term
        }
    	None
    }
    
  }

  def info(): StateInfo = NonLeaderInfo(getCluster.leader.toString())
  
  protected def getCluster: Cluster
  
  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {}
  
}