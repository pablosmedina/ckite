package the.walrus.ckite

import org.mapdb.DB
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.states.Follower
import the.walrus.ckite.states.Leader
import the.walrus.ckite.states.Candidate
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.states.State
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.AppendEntries
import java.util.concurrent.atomic.AtomicReference
import the.walrus.ckite.states.Starter
import the.walrus.ckite.states.Stopped

class LocalMember(binding: String, db: DB) extends Member(binding) {

  val state = new AtomicReference[State](Starter)
  val currentTerm = db.getAtomicInteger("term")
  val votedFor = db.getAtomicString("votedFor")

  def term(): Int = currentTerm.intValue()

  def voteForMyself = votedFor.set(id)

  def on(requestVote: RequestVote)(implicit cluster: Cluster): RequestVoteResponse = cluster.synchronized {
    if (requestVote.term < term) {
      LOG.debug(s"Rejecting vote to old candidate: ${requestVote}")
      RequestVoteResponse(term, false)
    } else {
      currentState on requestVote
    }
  }

  def updateTermIfNeeded(receivedTerm: Int)(implicit cluster: Cluster) = {
    if (receivedTerm > term) {
      LOG.debug(s"New term detected. Moving from ${term} to ${receivedTerm}.")
      votedFor.set("")
      currentTerm.set(receivedTerm)
      cluster.updateContextInfo
    }
  }

  def incrementTerm(implicit cluster: Cluster) = {
    val term = currentTerm.incrementAndGet()
    cluster.updateContextInfo
    term
  }

  def becomeLeader(term: Int)(implicit cluster: Cluster) = become(new Leader, term)

  def becomeCandidate(term: Int)(implicit cluster: Cluster) = become(new Candidate, term)

  def becomeFollower(term: Int)(implicit cluster: Cluster) = become(new Follower, term)

  private def become(newState: State, term: Int)(implicit cluster: Cluster) = {
    if (currentState.canTransition) {
      LOG.info(s"Transition from $state to $newState")
      currentState stop

      changeState(newState)

      currentState begin term
    }
  }

  def on(appendEntries: AppendEntries)(implicit cluster: Cluster): AppendEntriesResponse = currentState on appendEntries

  def on[T](command: Command)(implicit cluster: Cluster): T = currentState.on[T](command)

  def on(jointConsensusCommited: MajorityJointConsensus)(implicit cluster: Cluster) = currentState on jointConsensusCommited

  def currentState = state.get()

  private def changeState(newState: State) = state.set(newState)

  def stop(implicit cluster: Cluster) = {
    become(Stopped, cluster.local.term)
  }
}