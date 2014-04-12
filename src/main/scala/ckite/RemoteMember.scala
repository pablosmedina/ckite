package ckite

import java.util.concurrent.atomic.AtomicInteger
import ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import ckite.states.Follower
import ckite.rpc.WriteCommand
import ckite.states.Leader
import ckite.states.Candidate
import ckite.rpc.RequestVoteResponse
import ckite.util.Logging
import ckite.states.State
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.AppendEntries
import ckite.rpc.Connector
import ckite.rpc.thrift.ThriftConnector
import java.net.ConnectException
import com.twitter.finagle.ChannelWriteException
import ckite.rpc.AppendEntriesResponse
import ckite.states.Starter
import ckite.rpc.EnterJointConsensus
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import ckite.states.Stopped
import java.util.concurrent.atomic.AtomicBoolean
import ckite.rlog.Snapshot
import ckite.rpc.JoinResponse
import ckite.rpc.JoinRequest
import ckite.rpc.GetMembersResponse
import ckite.rpc.GetMembersRequest
import ckite.rpc.LogEntry
import ckite.rpc.AppendEntries
import java.util.concurrent.ConcurrentHashMap
import ckite.rpc.LogEntry

class RemoteMember(cluster: Cluster, binding: String) extends Member(binding) {

  LOG.debug(s"Creating RemoteMember $binding")
  
  val nextLogIndex = new AtomicInteger(1)
  val matchIndex = cluster.db.getAtomicInteger(s"$binding-matchIndex")
  val connector: Connector = new ThriftConnector(id)
  val replicationsEnabled = new AtomicBoolean(true)
  val replicationsInProgress = new ConcurrentHashMap[Int,Boolean]()
  private def localMember = cluster.local
  private def rlog = cluster.rlog

  override def forwardCommand[T](command: Command): T = {
    LOG.debug(s"Forward command $command to $id")
    connector.send[T](command)
  }

  def sendAppendEntries(term: Int) = {
    val request = createAppendEntries(term)
    connector.send(request).map { response =>
      LOG.trace(s"AppendEntries response $response")
      if (response.term > term) {
    	 receivedHigherTerm(response.term, term)
      } else {
        localMember.onAppendEntriesResponse(this, request, response)
      }
    }.recover { case e:Exception =>
      LOG.trace("Error sending appendEntries",e)
    }
    request.entries foreach { entry => replicationsInProgress.remove(entry.index) }
  }

  private def receivedHigherTerm(higherTerm: Int, oldTerm: Int) = {
    val currentTerm = localMember.term
    if (higherTerm > currentTerm) {
    	LOG.debug(s"Detected a term ${higherTerm} higher than current term ${currentTerm}. Step down")
    	localMember.stepDown(higherTerm)
    }
  }

  private def createAppendEntries(term: Int) = toReplicateEntries match {
    case head :: list => replication(term, head, list)
    case Nil => heartbeat(term)
  }

  private def replication(term: Int, head: LogEntry, list: List[LogEntry]) = {
    val toReplicate = head :: list
    LOG.trace(s"Replicating ${toReplicate.size} entries to $id")
    rlog.getPreviousLogEntry(head) match {
      case Some(previous) => normalReplication(term, previous, toReplicate)
      case None => firstReplication(term, toReplicate)
    }
  }
  
  private def normalReplication(term: Int, previous: LogEntry, entries: List[LogEntry]) = {
    AppendEntries(term, localMember.id, rlog.getCommitIndex, previous.index, previous.term, entries)
  }
  
  private def firstReplication(term: Int, toReplicate: List[LogEntry]) = {
    AppendEntries(term, localMember.id, rlog.getCommitIndex, entries = toReplicate)
  }
  
  private def heartbeat(term: Int) = AppendEntries(term, localMember.id, rlog.getCommitIndex)
  
  private def toReplicateEntries: List[LogEntry] = {
    val index = nextLogIndex.intValue()
    val entries = for (
      entry <- rlog.logEntry(index) if (isReplicationEnabled && !isBeingReplicated(index))
    ) yield entry
    List(entries).flatten
  }	
  
  private def isBeingReplicated(index: Int) = replicationsInProgress.put(index, true)
  
  def ackLogEntry(logEntryIndex: Int) = {
    updateMatchIndex(logEntryIndex)
    updateNextLogIndex
    replicationsInProgress.remove(logEntryIndex)
  }
  
  private def updateMatchIndex(logEntryIndex: Int) = {
    var currentMatchIndex = matchIndex.intValue()
	while(currentMatchIndex <= logEntryIndex && !matchIndex.compareAndSet(currentMatchIndex, logEntryIndex)) {
	     currentMatchIndex = matchIndex.intValue()
	}
  }
  
  private def updateNextLogIndex = {
    nextLogIndex.set(matchIndex.intValue() + 1)
  }

  def decrementNextLogIndex() = {
    val currentIndex = nextLogIndex.decrementAndGet()
    if (currentIndex == 0) nextLogIndex.set(1)
    replicationsInProgress.remove(nextLogIndex.intValue())
  }

  def sendSnapshot(snapshot: Snapshot) = {
      connector.send(snapshot)
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
    if (!wasEnabled) LOG.debug(s"Enabling replications to $id")
    wasEnabled
  }

  def disableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(false)
    if (wasEnabled) LOG.debug(s"Disabling replications to $id")
    wasEnabled
  }
  
  def join(joiningMemberId: String): JoinResponse = {
    LOG.debug(s"Joining with $id")
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