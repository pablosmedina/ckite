package ckite

import java.net.ConnectException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong }

import ckite.rlog.Snapshot
import ckite.rpc._
import ckite.rpc.thrift.FinagleThriftClient
import com.twitter.finagle.ChannelWriteException

import scala.Option.option2Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RemoteMember(cluster: Cluster, binding: String) extends Member(binding) {

  log.debug(s"Creating RemoteMember client for $binding")

  val nextLogIndex = new AtomicLong(1)
  val matchIndex = new AtomicLong(0)
  val client: RpcClient = new FinagleThriftClient(id)
  val replicationsEnabled = new AtomicBoolean(true)
  val replicationsInProgress = new ConcurrentHashMap[Long, Boolean]()
  private def rlog = cluster.rlog

  override def forwardCommand[T](command: Command): Future[T] = {
    log.debug(s"Forward command ${command} to ${id}")
    client.send[T](command)
  }

  def sendAppendEntries(term: Int) = {
    val request = createAppendEntries(term)
    client.send(request).map { response ⇒
      log.trace(s"AppendEntries response ${response} from ${id}")
      if (response.term > term) {
        receivedHigherTerm(response.term, term)
      } else {
        localMember.onAppendEntriesResponse(this, request, response)
      }
    }.recover {
      case e: Exception ⇒
        log.trace("Error sending appendEntries {}", e.getMessage())
    }.map { _ ⇒ request.entries foreach { markAsReplicated(_) } }
  }

  def markAsReplicated(entry: LogEntry): Unit = replicationsInProgress.remove(entry.index)

  private def localMember = cluster.local

  private def receivedHigherTerm(higherTerm: Int, oldTerm: Int) = {
    val currentTerm = localMember.term
    if (higherTerm > currentTerm) {
      log.debug("Detected a term {} higher than current term {}. Step down", higherTerm, currentTerm)
      localMember.stepDown(higherTerm)
    }
  }

  private def createAppendEntries(term: Int) = toReplicateEntries match {
    case head :: list ⇒ replication(term, head, list)
    case Nil          ⇒ heartbeat(term)
  }

  private def replication(term: Int, head: LogEntry, list: List[LogEntry]) = {
    val toReplicate = head :: list
    log.trace("Replicating {} entries to {}", toReplicate.size, id)
    rlog.getPreviousLogEntry(head) match {
      case Some(previous) ⇒ normalReplication(term, previous, toReplicate)
      case None           ⇒ firstReplication(term, toReplicate)
    }
  }

  private def normalReplication(term: Int, previous: LogEntry, entries: List[LogEntry]) = {
    AppendEntries(term, localMember.id, rlog.commitIndex, previous.index, previous.term, entries)
  }

  private def firstReplication(term: Int, toReplicate: List[LogEntry]) = {
    AppendEntries(term, localMember.id, rlog.commitIndex, entries = toReplicate)
  }

  private def heartbeat(term: Int) = AppendEntries(term, localMember.id, rlog.commitIndex)

  private def toReplicateEntries: List[LogEntry] = {
    val index = nextLogIndex.longValue()
    val entries = for (
      entry ← rlog.logEntry(index) if (isReplicationEnabled && !isBeingReplicated(index))
    ) yield entry
    List(entries).flatten
  }

  private def isBeingReplicated(index: Long) = replicationsInProgress.put(index, true)

  def isReplicationEnabled = replicationsEnabled.get()

  def ackLogEntry(index: Long) = {
    updateMatchIndex(index)
    updateNextLogIndex
    replicationsInProgress.remove(index)
  }

  private def updateMatchIndex(index: Long) = {
    var currentMatchIndex = matchIndex.longValue()
    while (currentMatchIndex < index && !matchIndex.compareAndSet(currentMatchIndex, index)) {
      currentMatchIndex = matchIndex.longValue()
    }
  }

  private def updateNextLogIndex = nextLogIndex.set(matchIndex.longValue() + 1)

  def decrementNextLogIndex() = {
    val currentIndex = nextLogIndex.decrementAndGet()
    if (currentIndex == 0) nextLogIndex.set(1)
    replicationsInProgress.remove(nextLogIndex.intValue())
  }

  def sendInstallSnapshot(installSnapshot: InstallSnapshot) = {
    client.send(installSnapshot)
  }

  def setNextLogIndex(index: Long) = nextLogIndex.set(index)

  def resetMatchIndex = matchIndex.set(0)

  /* If the candidate receives no response for an RPC, it reissues the RPC repeatedly until a response arrives or the election concludes */
  def requestVote(term: Int): Future[Boolean] = {
    log.debug(s"Requesting vote to $id")
    val lastLogEntry = rlog.getLastLogEntry()
    client.send(lastLogEntry match {
      case None        ⇒ RequestVote(localMember.id, term)
      case Some(entry) ⇒ RequestVote(localMember.id, term, entry.index, entry.term)
    }).map { voteResponse ⇒
      log.debug(s"Got $voteResponse from $id")
      voteResponse.granted && voteResponse.currentTerm == term
    } recover {
      case ChannelWriteException(e: ConnectException) ⇒
        log.debug(s"Can't connect to member $id")
        false
      case e: Exception ⇒
        log.error(s"Requesting vote: ${e.getMessage()}")
        false
    }
  }

  def enableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(true)
    if (!wasEnabled) log.debug(s"Enabling replications to $id")
    wasEnabled
  }

  def disableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(false)
    if (wasEnabled) log.debug(s"Disabling replications to $id")
    wasEnabled
  }

  def join(joiningMemberId: String): Future[JoinMemberResponse] = {
    log.debug(s"Joining with $id")
    client.send(JoinMember(joiningMemberId)).recover {
      case ChannelWriteException(e: ConnectException) ⇒
        log.debug(s"Can't connect to member $id")
        JoinMemberResponse(false)
    }
  }

}