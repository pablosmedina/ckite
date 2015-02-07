package ckite

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong }

import ckite.rpc.LogEntry.Index
import ckite.rpc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RemoteMember(rpc: Rpc, binding: String) extends Member(binding) {

  logger.debug(s"Creating RemoteMember client for $binding")

  val nextLogIndex = new AtomicLong(1)
  val matchIndex = new AtomicLong(0)

  private val client: RpcClient = rpc.createClient(id)

  private val replicationsEnabled = new AtomicBoolean(true)
  private val replicationsInProgress = new ConcurrentHashMap[Long, Boolean]()

  override def forwardCommand[T](command: Command): Future[T] = {
    logger.debug(s"Forward command ${command} to ${id}")
    client.send[T](command)
  }

  def sendAppendEntries(appendEntries: AppendEntries) = {
    client.send(appendEntries)
  }

  def sendRequestVote(requestVote: RequestVote) = {
    client.send(requestVote)
  }

  private def markAsReplicated(index: Index): Unit = replicationsInProgress.remove(index)

  def canReplicateIndex(index: Index): Boolean = isReplicationEnabled && !isBeingReplicated(index)

  private def isBeingReplicated(index: Long) = replicationsInProgress.put(index, true)

  private def isReplicationEnabled = replicationsEnabled.get()

  def acknowledgeIndex(index: Long) = {
    updateMatchIndex(index)
    updateNextLogIndex(index)
    markAsReplicated(index)
  }

  def markReplicationsNotInProgress(indexes: List[Long]) = {
    indexes.foreach(index ⇒ replicationsInProgress.remove(index))
  }

  private def updateMatchIndex(index: Long) = {
    var currentMatchIndex = matchIndex.longValue()
    while (currentMatchIndex < index && !matchIndex.compareAndSet(currentMatchIndex, index)) {
      currentMatchIndex = matchIndex.longValue()
    }
  }

  private def updateNextLogIndex(index: Long) = nextLogIndex.set(index + 1)

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

  def enableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(true)
    if (!wasEnabled) logger.debug(s"Enabling replications to $id")
    wasEnabled
  }

  def disableReplications() = {
    val wasEnabled = replicationsEnabled.getAndSet(false)
    if (wasEnabled) logger.debug(s"Disabling replications to $id")
    wasEnabled
  }

  def join(joiningMemberId: String): Future[JoinMemberResponse] = {
    logger.debug(s"Joining with $id")
    client.send(JoinMember(joiningMemberId)).recover {
      case reason: Throwable ⇒
        logger.warn(s"Can't join to member $id", reason)
        JoinMemberResponse(false)
    }
  }

}