package ckite

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import ckite.rlog._
import ckite.rpc.LogEntry.{ Term, Index }
import ckite.rpc._
import ckite.statemachine.StateMachine
import ckite.util.{ CustomThreadFactory, LockSupport, Logging }

import scala.Option.option2Iterable
import scala.collection.immutable.NumericRange
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future, Promise }
import ckite.util.CKiteConversions._

case class RLog(raft: Raft, stateMachine: StateMachine, storage: Storage, configuration: Configuration) extends Logging with LockSupport {

  private def consensus = raft.consensus
  private def membership = raft.membership

  val log = storage.log()
  private val lastIndexAtomic = new AtomicLong(0)

  val applyPromises = new ConcurrentHashMap[Long, Promise[_]]()
  val snapshotManager = new SnapshotManager(membership, this, storage, configuration)
  val asyncPool = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), CustomThreadFactory("LogAppender-worker"))
  val asyncExecutionContext = ExecutionContext.fromExecutor(asyncPool)
  private val appendsQueue = new LinkedBlockingQueue[Append[_]]()

  private val commandApplier = new CommandApplier(consensus, this, stateMachine)

  def bootstrap() = {
    assertEmptyLog
    assertNoSnapshot
  }

  //Leader append
  def append[T](term: Term, write: WriteCommand[T]): Future[(LogEntry, Promise[T])] = {
    val appendFuture = leaderAppend[T](term, write)
    applyLogCompactionPolicy
    appendFuture
  }

  //Follower append
  def tryAppend(appendEntries: AppendEntries): Future[Boolean] = {
    logger.trace("Try appending {}", appendEntries)
    val canAppend = hasPreviousLogEntry(appendEntries)
    if (canAppend) {
      appendAll(appendEntries.entries) map { _ ⇒
        commandApplier.commit(appendEntries.commitIndex)
        applyLogCompactionPolicy()
        canAppend
      }
    } else {
      logger.trace("Rejecting {}", appendEntries)
      Future.successful(canAppend)
    }
  }

  private def applyLogCompactionPolicy() = snapshotManager.applyLogCompactionPolicy

  private def hasPreviousLogEntry(appendEntries: AppendEntries) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }

  //Follower appends all these entries and waits for them to be flushed to the persistentLog
  private def appendAll(entries: List[LogEntry]): Future[List[Index]] = {
    val appendPromises = entries.map { entry ⇒
      logger.trace(s"Try appending $entry")
      if (!containsEntry(entry.index, entry.term)) {
        if (hasIndex(entry.index)) {
          //If an entry is overridden then all the subsequent entries must be removed
          logger.debug("Will discard inconsistent entries starting from index #{} to follow Leader's log", entry.index)
          shared {
            log.discardEntriesFrom(entry.index)
          }
        }
        Some(followerAppend(entry))
      } else {
        logger.debug("Discarding append of a duplicate entry {}", entry)
        None
      }
    }
    composeFutures(appendPromises)
  }

  private def composeFutures(appendPromises: List[Option[Future[Index]]]) = {
    val futures = for {
      append ← appendPromises
      future ← append
    } yield future
    Future.sequence(futures)
  }

  private def hasIndex(index: Long) = log.getLastIndex >= index

  def commit(index: Long): Unit = commandApplier.commit(index)

  def commit(logEntry: LogEntry): Unit = commit(logEntry.index)

  def execute[T](command: ReadCommand[T]) = commandApplier.applyRead(command)

  def entry(index: Long, allowCompactedEntry: Boolean = false): Option[LogEntry] = {
    val entry = log.getEntry(index)
    if (entry != null) Some(entry)
    else if (allowCompactedEntry && snapshotManager.isInSnapshot(index)) Some(snapshotManager.compactedEntry)
    else None
  }

  def lastEntry: Option[LogEntry] = {
    val lastLogIndex = findLastLogIndex
    if (snapshotManager.isInSnapshot(lastLogIndex)) {
      Some(snapshotManager.compactedEntry)
    } else {
      entry(lastLogIndex)
    }
  }

  def getPreviousLogEntry(logEntry: LogEntry): Option[LogEntry] = entry(logEntry.index - 1, true)

  def containsEntry(index: Long, term: Int) = {
    val logEntryOption = entry(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (isZeroEntry(index, term) || snapshotManager.isInSnapshot(index, term))
  }

  private def isZeroEntry(index: Long, term: Int): Boolean = index == -1 && term == -1

  def resetLastIndex() = lastIndexAtomic.set(findLastLogIndex)

  def findLastLogIndex(): Long = {
    val lastIndex = log.getLastIndex
    if (lastIndex > 0) lastIndex else snapshotManager.latestSnapshotIndex
  }

  def commitIndex: Long = commandApplier.commitIndex

  def nextLogIndex() = lastIndexAtomic.incrementAndGet()

  def size() = log.size

  def stop() = {
    stopAppender
    commandApplier.stop
    log.close()
  }

  def serializeStateMachine = stateMachine.serialize()

  def assertEmptyLog = {
    if (log.size > 0) throw new IllegalStateException("Log is not empty")
  }

  def assertNoSnapshot = {
    if (snapshotManager.latestSnapshotIndex > 0) throw new IllegalStateException("A Snapshot was found")
  }

  def initialize() = {
    logger.info("Initializing...")

    restoreLatestClusterConfiguration

    replay

    startAppender

    logger.info("Done initializing")
  }

  def replay {
    val lastAppliedIndex: Long = reloadSnapshot

    commandApplier.start(lastAppliedIndex)

    commandApplier.replay
  }

  def reloadSnapshot: Long = {
    val latestSnapshot = snapshotManager.latestSnapshot
    val lastAppliedIndex: Long = latestSnapshot map { snapshot ⇒
      logger.info("Found a {}", snapshot)
      if (snapshot.index > commandApplier.lastApplied) {
        logger.info("The Snapshot has more recent data than the StateMachine. Will reload it...")
        snapshotManager.reload(snapshot)
        snapshot.index
      } else {
        logger.info("The StateMachine has more recent data than the Snapshot")
        membership.transitionTo(snapshot.clusterConfiguration)
        commandApplier.lastApplied
      }
    } getOrElse {
      logger.info("No Snapshot was found")
      0
    }
    lastAppliedIndex
  }

  def restoreLatestClusterConfiguration {
    val latestClusterConfigurationEntry = findLatestClusterConfiguration
    latestClusterConfigurationEntry foreach { entry ⇒
      logger.info("Found cluster configuration in the log: {}", entry.command)
      consensus.membership.changeConfiguration(entry.index, entry.command.asInstanceOf[ClusterConfigurationCommand])
    }
  }

  private def findLatestClusterConfiguration: Option[LogEntry] = {
    traversingInReversal find { index ⇒
      val logEntry = entry(index)
      if (!logEntry.isDefined) return None
      logEntry.collect { case LogEntry(term, entry, c: ClusterConfigurationCommand) ⇒ true }.getOrElse(false)
    } map { index ⇒ entry(index) } flatten
  }

  def traversingInReversal: NumericRange[Long] = {
    findLastLogIndex to 1 by -1
  }

  def rollLog(index: Long) = exclusive {
    log.rollLog(index)
  }

  def lastIndex(): Index = lastIndexAtomic.longValue()

  def isEmpty: Boolean = lastIndex == 0

  def startAppender = asyncExecutionContext.execute(asyncAppend _)

  def stopAppender = {
    asyncPool.shutdownNow()
    asyncPool.awaitTermination(10, TimeUnit.SECONDS)
  }

  //leader append
  def leaderAppend[T](term: Int, write: WriteCommand[T]): Future[(LogEntry, Promise[T])] = append(LeaderAppend[T](term, write))

  //follower append
  def followerAppend(entry: LogEntry): Future[Long] = append(FollowerAppend(entry))

  private def append[T](append: Append[T]): Future[T] = {
    logger.trace(s"Append $append")
    val promise: Promise[T] = append.promise
    appendsQueue.offer(append)
    promise.future
  }

  private def asyncAppend = {
    try {
      while (true) {
        val append = next

        logger.debug(s"Appending ${append.logEntry}")
        val logEntry = append.logEntry

        shared {
          log.append(logEntry).map { _ ⇒
            onLogEntryAppended(append)(logEntry)
          }
        }
      }
    } catch {
      case e: InterruptedException ⇒ logger.info("Shutdown LogAppender...")
    }
  }

  private def onLogEntryAppended(append: Append[_])(entry: LogEntry) = {
    entry.command match {
      case configuration: ClusterConfigurationCommand ⇒ membership.changeConfiguration(entry.index, configuration)
      case _ ⇒ ;
    }
    append.onComplete(entry)
  }

  private def next: Append[_] = {
    if (appendsQueue.isEmpty()) {
      appendsQueue.take()
    } else {
      appendsQueue.poll()
    }
  }

  trait Append[T] {
    def promise: Promise[T]

    def logEntry: LogEntry

    def onComplete(logEntry: LogEntry)
  }

  case class LeaderAppend[T](term: Int, write: WriteCommand[T]) extends Append[(LogEntry, Promise[T])] {
    val _promise = Promise[(LogEntry, Promise[T])]()
    val _valuePromise = Promise[T]()

    def promise = _promise

    val logEntry = {
      val logEntry = LogEntry(term, nextLogIndex, write)
      applyPromises.put(logEntry.index, _valuePromise)
      logEntry
    }

    def onComplete(logEntry: LogEntry) = _promise.success((logEntry, _valuePromise))
  }

  case class FollowerAppend(entry: LogEntry) extends Append[Long] {
    val _promise = Promise[Long]()

    def promise = _promise

    def logEntry = entry

    def onComplete(logEntry: LogEntry) = _promise.success(logEntry.index)
  }

}