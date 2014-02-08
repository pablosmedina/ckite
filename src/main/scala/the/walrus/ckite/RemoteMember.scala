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
import the.walrus.ckite.rpc.JoinResponse
import the.walrus.ckite.rpc.JoinRequest
import the.walrus.ckite.rpc.GetMembersResponse
import the.walrus.ckite.rpc.GetMembersRequest
import the.walrus.ckite.rpc.LogEntry

class RemoteMember(cluster: Cluster, binding: String) extends Member(binding) {

  LOG.info(s"Creating RemoteMember $binding")
  
  val nextLogIndex = new AtomicInteger(1)
  val connector: Connector = new ThriftConnector(id)
  val replicationsEnabled = new AtomicBoolean(true)
  
  private def localMember = cluster.local
  private def rlog = cluster.rlog

  override def forwardCommand[T](command: Command): T = {
    LOG.debug(s"Forward command $command to $id")
    connector.send[T](command)
  }

  def sendHeartbeat(term: Int) = synchronized {
    LOG.trace(s"Sending heartbeat to $id in term $term")
    val appendEntries = createAppendEntries(term)
    connector.send(appendEntries).map {
      appendEntriesResponse =>
        if (appendEntriesResponse.term > term) {
          LOG.debug(s"Detected a term ${appendEntriesResponse.term} higher than current term ${term}. Step down")
          localMember.stepDown(None, term)
        } else {
          localMember.onAppendEntriesResponse(this, appendEntries, appendEntriesResponse)
        }
    }
  }

  private def createAppendEntries(termToSent: Int): AppendEntries = {
    val entryToPiggyBack = if (isReplicationEnabled) rlog.getLogEntry(nextLogIndex.intValue()) else None
    entryToPiggyBack match {
      case None => AppendEntries(termToSent, localMember.id, rlog.getCommitIndex)
      case Some(entry) => {
        val entriesToPiggyBack = List(entry)
        val appendEntriesMessage = rlog.getPreviousLogEntry(entriesToPiggyBack(0)) match {
          case None => AppendEntries(termToSent, localMember.id, rlog.getCommitIndex, entries = entriesToPiggyBack)
          case Some(previousEntry) => AppendEntries(termToSent, localMember.id, rlog.getCommitIndex, previousEntry.index, previousEntry.term, entriesToPiggyBack)
        }
        LOG.trace(s"Piggybacking entry $entry to $id. Message is $appendEntriesMessage")
        appendEntriesMessage
      }
    }
  }

  def replicate(logEntry: LogEntry): Boolean = synchronized {
    if (!isReplicationEnabled) {
      LOG.info(s"Replication is not enabled. Could not replicate $logEntry")
      return false
    }
    if (logEntry.index < nextLogIndex.intValue()) {
      LOG.info(s"LogEntry $logEntry was already replicated to $id")
      return true
    }
    LOG.info(s"Replicating to $id")
    val appendEntries = createAppendEntries(cluster.local.term)
      connector.send(appendEntries).map { replicationResponse =>
        LOG.debug(s"Got replication response $replicationResponse from $id")
        localMember.onAppendEntriesResponse(this, appendEntries, replicationResponse)
        replicationResponse.success
      }.recover {
        case ChannelWriteException(e: ConnectException) =>
          LOG.debug(s"Can't connect to member $id")
          false
        case e: Exception =>
          LOG.error(s"Error replicating to $id: ${e.getMessage()}", e)
          false
      } get
  }

  def sendSnapshot(snapshot: Snapshot) = synchronized {
    cluster.inContext {
      val future = connector.send(snapshot)
      future.get
      setNextLogIndex(snapshot.lastLogEntryIndex + 1)
      enableReplications()
    }
  }

  def setNextLogIndex(index: Int) = nextLogIndex.set(index)

  /* If the candidate receives no response for an RPC, it reissues the RPC repeatedly until a response arrives or the election concludes */
  def requestVote: Boolean = {
    LOG.debug(s"Requesting vote to $id")
    val lastLogEntry = rlog.getLastLogEntry()
    connector.send(lastLogEntry match {
      case None => RequestVote(localMember.id, localMember.term)
      case Some(entry) => RequestVote(localMember.id, localMember.term, entry.index, entry.term)
    }).map { voteResponse =>
      LOG.debug(s"Got $voteResponse from $id")
      voteResponse.granted
    } recover {
      case ChannelWriteException(e: ConnectException) =>
        LOG.debug(s"Can't connect to member $id")
        false
      case e: Exception =>
        LOG.error(s"Requesting vote: ${e.getMessage()}")
        false
    } get
  }

  def enableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(true)
    if (!wasEnabled) LOG.info(s"Enabling replications to $id")
    wasEnabled
  }

  def disableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(false)
    if (wasEnabled) LOG.info(s"Disabling replications to $id")
    wasEnabled
  }
  
  def join(joiningMemberId: String): JoinResponse = {
    LOG.info(s"Joining with $id")
    connector.send(JoinRequest(joiningMemberId)).recover {
      case ChannelWriteException(e: ConnectException) =>
        LOG.debug(s"Can't connect to member $id")
        JoinResponse(false)
    } get
  }
  
  def getMembers(): GetMembersResponse = {
    connector.send(GetMembersRequest()).recover {
      case ChannelWriteException(e: ConnectException) =>
        LOG.debug(s"Can't connect to member $id")
        GetMembersResponse(false, Seq())
    } get
  }

  def isReplicationEnabled = replicationsEnabled.get()

}