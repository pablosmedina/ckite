package the.walrus.ckite

import java.util.concurrent.atomic.AtomicInteger
import the.walrus.ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import the.walrus.ckite.states.Follower
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.states.Leader
import the.walrus.ckite.states.Candidate
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.rest.RestConnector
import the.walrus.ckite.states.State
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.Connector

class Member(val host: String, val port: String) extends Logging {

  val currentTerm = new AtomicInteger(0)
  val nextLogIndex = new AtomicInteger(0)
  val state = new AtomicReference[State](Follower)
  val connector: Connector = new RestConnector()
  val votedFor = new AtomicReference[Option[String]]

  def sendHeartbeat(term: Int)(implicit cluster: Cluster) = {
    LOG.trace(s"Sending heartbeat to $id in term ${term}")
    connector.send(this, createAppendEntries(term)).map {
      appendEntriesResponse =>
        if (appendEntriesResponse.term > term) {
          LOG.info(s"Detected a term ${appendEntriesResponse.term} higher than current term ${term}. Step down")
          cluster.local.currentState.stepDown(None, term)
        }
    }
  }

  private def createAppendEntries(termToSent: Int)(implicit cluster: Cluster): AppendEntries = {
    val logEntryToPiggyBack = RLog.getLogEntry(nextLogIndex.intValue())
    logEntryToPiggyBack match {
      case None => AppendEntries(termToSent, cluster.local.id, RLog.getCommitIndex)
      case Some(entry) => {
        val entriesToPiggyBack = List(entry)
        RLog.getPreviousLogEntry(entriesToPiggyBack(0)) match {
          case None => AppendEntries(termToSent, cluster.local.id, RLog.getCommitIndex, entries = entriesToPiggyBack)
          case Some(previousEntry) => AppendEntries(termToSent, cluster.local.id, RLog.getCommitIndex, previousEntry.term, previousEntry.index, entriesToPiggyBack)
        }
      }
    }
  }

  def onAppendEntriesReceived(appendEntries: AppendEntries)(implicit cluster: Cluster): AppendEntriesResponse = {
    currentState().onAppendEntriesReceived(appendEntries)
  }

  def term(): Int = {
    this.currentTerm.intValue()
  }

  def id() = {
    s"$host:$port"
  }

  def updateTermIfNeeded(receivedTerm: Int)(implicit cluster: Cluster) = {
    if (receivedTerm > term) {
      LOG.info(s"New term detected. Moving from ${term} to ${receivedTerm}.")
      votedFor.set(None)
      currentTerm.set(receivedTerm)
      cluster.updateContextInfo()
    }
  }

  def incrementTerm(implicit cluster: Cluster) = {
    val term = currentTerm.incrementAndGet()
    cluster.updateContextInfo()
    term
  }

  def onMemberRequestingVoteReceived(requestVote: RequestVote)(implicit cluster: Cluster): RequestVoteResponse = {
    cluster.synchronized {
      if (requestVote.term < term) {
        LOG.debug(s"Rejecting vote to old candidate: ${requestVote}")
        RequestVoteResponse(term, false)
      } else {
        currentState.onRequestVoteReceived(requestVote)
      }
    }
  }

  /* If the candidate receives no response for an RPC, it reissues the RPC repeatedly until a response arrives or the election concludes */
  def requestVote(implicit cluster: Cluster): Boolean = {
    LOG.info(s"Requesting vote to $id")
    val lastLogEntry = RLog.getLastLogEntry()
    connector.send(this, lastLogEntry match {
      case None => RequestVote(cluster.local.id, cluster.local.term)
      case Some(entry) => RequestVote(cluster.local.id, cluster.local.term, entry.index, entry.term)
    }).map { voteResponse =>
      LOG.info(s"Got Request vote response: $voteResponse")
      voteResponse.granted
    } recover {
      case e: Exception =>
        LOG.error(s"Error requesting vote: ${e.getMessage()}")
        false
    } get
  }

  def forwardCommand(command: Command) = {
    connector.send(this, command)
  }

  def replicate(appendEntries: AppendEntries) = {
    connector.send(this, appendEntries).map { replicationResponse =>
      if (replicationResponse.success) {
        nextLogIndex.incrementAndGet()
      } else {
        nextLogIndex.decrementAndGet()
      }
      replicationResponse.success
    }.recover {
      case e: Exception =>
        LOG.error(s"Error replicating: ${e.getMessage()}")
        false
    } get
  }

  def onCommandReceived(command: Command)(implicit cluster: Cluster) = {
    currentState().onCommandReceived(command)
  }

  def becomeLeader(term: Int)(implicit cluster: Cluster) = {
    become(Leader, term)
  }

  def becomeCandidate(term: Int)(implicit cluster: Cluster) = {
    become(Candidate, term)
  }

  def becomeFollower(term: Int)(implicit cluster: Cluster) = {
    become(Follower, term)
  }

  def setNextLogIndex(index: Int) = {
    nextLogIndex.set(index)
  }
  
  def voteForMyself() = {
    votedFor.set(Some(id))
  }

  private def become(newState: State, term: Int)(implicit cluster: Cluster) = {
    LOG.info(s"Transition from $state to $newState")
    currentState().stop
    changeState(newState)
    currentState().begin(term)
  }

  private def currentState() = {
    state.get()
  }

  private def changeState(newState: State) = {
    state.set(newState)
  }

  override def toString() = {
    id
  }
}

