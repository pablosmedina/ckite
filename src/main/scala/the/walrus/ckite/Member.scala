package the.walrus.ckite

import java.util.concurrent.atomic.AtomicInteger
import the.walrus.ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import the.walrus.ckite.states.Follower
import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.states.Leader
import the.walrus.ckite.states.Candidate
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.util.Logging
import the.walrus.ckite.states.State
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.Connector
import the.walrus.ckite.rpc.thrift.ThriftConnector
import java.net.ConnectException
import com.twitter.finagle.ChannelWriteException
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.states.Starter
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.states.Stopped
import java.util.concurrent.atomic.AtomicBoolean
import the.walrus.ckite.rlog.Snapshot

class Member(val binding: String) extends Logging {

  val nextLogIndex = new AtomicInteger(1)
  val connector: Connector = new ThriftConnector(binding)
  val replicationsEnabled = new AtomicBoolean(true)

  def id() = s"$binding"
  
  def forwardCommand[T](command: Command): T = connector.send[T](this, command)
  
  def sendHeartbeat(term: Int)(implicit cluster: Cluster) = synchronized {
    LOG.trace(s"Sending heartbeat to $id in term ${term}")
    val appendEntries = createAppendEntries(term)
    connector.send(this, appendEntries).map {
      appendEntriesResponse =>
        if (appendEntriesResponse.term > term) {
          LOG.debug(s"Detected a term ${appendEntriesResponse.term} higher than current term ${term}. Step down")
          cluster.local.currentState.stepDown(None, term)
        } else {
          cluster.local.currentState.onAppendEntriesResponse(this, appendEntries, appendEntriesResponse)
        }
    }
  }

  private def createAppendEntries(termToSent: Int)(implicit cluster: Cluster): AppendEntries =  {
    val entryToPiggyBack = if (isReplicationEnabled) cluster.rlog.getLogEntry(nextLogIndex.intValue()) else None
    entryToPiggyBack match {
      case None => AppendEntries(termToSent, cluster.local.id, cluster.rlog.getCommitIndex)
      case Some(entry) => {
        val entriesToPiggyBack = List(entry)
        val appendEntriesMessage = cluster.rlog.getPreviousLogEntry(entriesToPiggyBack(0)) match {
          case None => AppendEntries(termToSent, cluster.local.id, cluster.rlog.getCommitIndex, entries = entriesToPiggyBack)
          case Some(previousEntry) => AppendEntries(termToSent, cluster.local.id, cluster.rlog.getCommitIndex, previousEntry.index, previousEntry.term, entriesToPiggyBack)
        }
        LOG.trace(s"Piggybacking entry $entry to $id. Message is $appendEntriesMessage")
        appendEntriesMessage
      }
    }
  }
  
  def replicate(appendEntries: AppendEntries)(implicit cluster: Cluster): Boolean =  { 
    if (!isReplicationEnabled) return false
    LOG.info(s"Replicating to $id")
    synchronized {
    connector.send(this, appendEntries).map { replicationResponse =>
      cluster.local.currentState.onAppendEntriesResponse(this, appendEntries, replicationResponse)
      replicationResponse.success
    }.recover {
      case ChannelWriteException(e:ConnectException)  =>
        LOG.debug(s"Can't connect to member $id")
        false
      case e: Exception =>
        LOG.error(s"Error replicating to $id: ${e.getMessage()}",e)
        false
    } get
  }
  }
  
  def sendSnapshot(snapshot: Snapshot) = {
      connector.send(this, snapshot)
  }
  
  
  def setNextLogIndex(index: Int) = nextLogIndex.set(index)


  /* If the candidate receives no response for an RPC, it reissues the RPC repeatedly until a response arrives or the election concludes */
  def requestVote(implicit cluster: Cluster): Boolean = {
    LOG.debug(s"Requesting vote to $id")
    val lastLogEntry = cluster.rlog.getLastLogEntry()
    connector.send(this, lastLogEntry match {
      case None => RequestVote(cluster.local.id, cluster.local.term)
      case Some(entry) => RequestVote(cluster.local.id, cluster.local.term, entry.index, entry.term)
    }).map { voteResponse =>
      LOG.debug(s"Got Request vote response: $voteResponse")
      voteResponse.granted
    } recover {
      case ChannelWriteException(e:ConnectException)  =>
        LOG.debug(s"Can't connect to member $id")
        false
      case e: Exception => 
        LOG.error(s"Requesting vote: ${e.getMessage()}")
        false
    } get
  }
  
  def enableReplications() = {
	LOG.info(s"Enabling replications to $id")
    replicationsEnabled.set(true)
  }
  
  def disableReplications() = {
    LOG.info(s"Disabling replications to $id")
    replicationsEnabled.set(false)
  }
  
  def isReplicationEnabled = replicationsEnabled.get()
  

  override def toString() = id
  
}