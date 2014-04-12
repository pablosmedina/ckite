package ckite

import ckite.rpc.LogEntry
import ckite.rpc.WriteCommand
import java.util.concurrent.atomic.AtomicInteger
import ckite.util.Logging
import ckite.statemachine.StateMachine
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import ckite.rpc.AppendEntries
import ckite.rpc.EnterJointConsensus
import ckite.rpc.LeaveJointConsensus
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.EnterJointConsensus
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.mapdb.DBMaker
import java.io.File
import org.mapdb.DB
import ckite.rlog.Snapshot
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.concurrent.SynchronousQueue
import ckite.util.CKiteConversions._
import ckite.rpc.NoOp
import com.twitter.util.Future
import ckite.exception.WriteTimeoutException
import ckite.rpc.CompactedEntry
import scala.concurrent.Promise
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent._
import ckite.rlog.MapDBPersistentLog
import ckite.rlog.LogCompactor
import java.util.concurrent.atomic.AtomicBoolean
import ckite.rlog.FixedSizeLogCompactionPolicy
import ckite.statemachine.CommandExecutor

class RLog(val cluster: Cluster, stateMachine: StateMachine) extends Logging {

  val persistentLog = new MapDBPersistentLog(cluster.db)
  val commitIndex = cluster.db.getAtomicInteger("commitIndex")

  val lastLog = new AtomicInteger(0)

  val lock = new ReentrantReadWriteLock()
  val exclusiveLock = lock.writeLock()
  val sharedLock = lock.readLock()

  val commitPromises = new ConcurrentHashMap[Int, Promise[Any]]()

  val asyncExecutionContext = ExecutionContext.fromExecutor(new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("AsyncWorker", true)))

  val logCompactionPolicy = new FixedSizeLogCompactionPolicy(cluster.configuration.fixedLogSizeCompaction)
  val logCompactor = new LogCompactor(this)

  val commandExecutor = new CommandExecutor(stateMachine)

  initialize()

  def tryAppend(appendEntries: AppendEntries) = {
    LOG.trace(s"Try appending $appendEntries")
    val canAppend = hasPreviousLogEntry(appendEntries)
    if (canAppend) {
      shared {
        appendWithLockAcquired(appendEntries.entries)
        safeCommitEntriesUntil(appendEntries.commitIndex)
      }
      applyLogCompactionPolicy
    }
    canAppend
  }

  private def append(logEntry: LogEntry): Promise[Any] = {
    val promise = Promise[Any]()
    commitPromises.put(logEntry.index, promise)
    append(List(logEntry))
    promise
  }

  //appends needs to be serialized to avoid holes
  def append(write: WriteCommand): (LogEntry, Promise[Any]) = exclusive {
    val logEntry = LogEntry(cluster.local.term, cluster.rlog.nextLogIndex, write)
    (logEntry, append(logEntry))
  }

  private def append(logEntries: List[LogEntry]) = {
    appendWithLockAcquired(logEntries)
    applyLogCompactionPolicy
  }

  private def applyLogCompactionPolicy = {
    if (logCompactionPolicy.applies(persistentLog, stateMachine)) {
      logCompactor.asyncCompact
    }
  }

  private def hasPreviousLogEntry(appendEntries: AppendEntries) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }

  private def appendWithLockAcquired(logEntries: List[LogEntry]) = {
    logEntries.foreach { entry =>
      if (!containsEntry(entry.index, entry.term)) {
        //If an entry is overridden then all the subsequent entries must be removed
        logEntry(entry.index) foreach { entryToOverride =>
          removeSubsequentEntries(entryToOverride.index)
        }
        LOG.debug(s"Appending $entry")
        persistentLog.append(entry)
        afterAppend(entry)
      } else {
        LOG.debug(s"Discarding append of a duplicate entry $entry")
      }
    }
  }

  private def removeSubsequentEntries(index: Int) = {
    (index to persistentLog.size) foreach { indexToRemove =>
      LOG.debug(s"Removing uncommitted index #$indexToRemove due to override by the Leader")
      persistentLog.remove(indexToRemove)
    }
  }

  private def afterAppend(logEntry: LogEntry) = {
    logEntry.command match {
      case c: EnterJointConsensus => cluster.apply(c)
      case c: LeaveJointConsensus => cluster.apply(c)
      case _ => ;
    }
  }

  def commit(index: Int): Unit = shared {
    logEntry(index).map { entry =>
      if (entry.term == cluster.local.term) {
        safeCommitEntriesUntil(index)
      } else {
        LOG.warn(s"Unsafe to commit an old term entry: $entry")
      }
    } orElse {
      raiseMissingLogEntryException(index)
    }
  }

  def commit(logEntry: LogEntry): Unit = commit(logEntry.index)

  private def safeCommitEntriesUntil(entryIndex: Int) = {
    val start = commitIndex.intValue() + 1
    val end = entryIndex
    (start to end) foreach { index => safeCommit(index) }
  }

  private def safeCommit(index: Int) = withLogEntry(index) { entry =>
    entry.synchronized {
      if (!isCommitted(index)) {
        LOG.debug(s"Committing $entry")
        val value = execute(entry.command)
        commitIndex.set(entry.index)
        val promise = commitPromises.get(index)
        if (promise != null) { //Is this possible?
          promise.success(value)
          commitPromises.remove(index)
        }
      } else {
        LOG.debug(s"Already committed entry $entry")
      }
    }
  }

  private def isCommitted(index: Int) = index <= commitIndex.intValue()

  def execute(command: Command)(implicit context: ExecutionContext = asyncExecutionContext) = {
    LOG.debug(s"Executing $command")
    shared {
      command match {
        case c: EnterJointConsensus => {
          future {
            try {
              cluster.on(MajorityJointConsensus(c.newBindings))
            } catch {
              case e: WriteTimeoutException => LOG.warn(s"Could not commit LeaveJointConsensus")
            }
          }
          true
        }
        case c: LeaveJointConsensus => true
        case c: NoOp => ;
        case _ => applyCommand(command)
      }
    }
  }

  private def applyCommand(command: Command) = commandExecutor.apply(command)

  def execute(command: ReadCommand) = applyCommand(command)

  def logEntry(index: Int, allowCompactedEntry: Boolean = false): Option[LogEntry] = {
    val entry = persistentLog.getEntry(index)
    if (entry != null) Some(entry) else {
      getSnapshot().map { snapshot =>
        if (snapshot.lastLogEntryIndex == index && allowCompactedEntry) {
          Some(compactedEntry(snapshot))
        } else
          None
      }.getOrElse(None)
    }
  }

  private def withLogEntry[T](index: Int)(block: LogEntry => T) = logEntry(index) foreach block

  def getLastLogEntry(): Option[LogEntry] = {
    val lastLogIndex = findLastLogIndex
    if (isInSnapshot(lastLogIndex)) return {
      val snapshot = getSnapshot().get
      return Some(compactedEntry(snapshot))
    }
    logEntry(lastLogIndex)
  }

  def getPreviousLogEntry(entry: LogEntry): Option[LogEntry] = logEntry(entry.index - 1, true)

  def containsEntry(index: Int, term: Int) = {
    val logEntryOption = logEntry(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (isZeroEntry(index, term) || isInSnapshot(index, term))
  }

  private def compactedEntry(snapshot: Snapshot) = LogEntry(snapshot.lastLogEntryTerm, snapshot.lastLogEntryIndex, CompactedEntry())

  private def isZeroEntry(index: Int, term: Int): Boolean = index == -1 && term == -1

  private def isInSnapshot(index: Int, term: Int): Boolean = shared {
    getSnapshot().map { snapshot => snapshot.lastLogEntryTerm >= term && snapshot.lastLogEntryIndex >= index }
      .getOrElse(false)
  }

  private def isInSnapshot(index: Int): Boolean = shared {
    getSnapshot().map { snapshot => snapshot.lastLogEntryIndex >= index }
      .getOrElse(false)
  }

  def resetLastLog() = lastLog.set(findLastLogIndex)

  def findLastLogIndex(): Int = persistentLog.getLastIndex

  def getCommitIndex(): Int = commitIndex.intValue()

  def nextLogIndex() = lastLog.incrementAndGet()

  def size() = persistentLog.size

  def installSnapshot(snapshot: Snapshot): Boolean = exclusive {
    LOG.debug(s"Installing $snapshot")
    val snapshots = cluster.db.getTreeMap[Long, Array[Byte]]("snapshots")
    snapshots.put(System.currentTimeMillis(), snapshot.serialize)
    stateMachine.deserialize(snapshot.stateMachineState)
    commitIndex.set(snapshot.lastLogEntryIndex)
    snapshot.membership.recoverIn(cluster)
    LOG.debug(s"Finished installing $snapshot")
    true
  }

  private def initialize() = {
    val nextIndexAfterSnapshot = reloadSnapshot()
    val currentCommitIndex = commitIndex.get()
    if (nextIndexAfterSnapshot <= currentCommitIndex) {
      replay(nextIndexAfterSnapshot, currentCommitIndex)
    } else {
      LOG.debug(s"No entries to be replayed")
    }
  }

  private def reloadSnapshot(): Int = {
    val lastSnapshot = getSnapshot()
    if (lastSnapshot.isDefined) {
      val snapshot = lastSnapshot.get
      LOG.debug(s"Installing $snapshot")
      stateMachine.deserialize(snapshot.stateMachineState)
      snapshot.membership.recoverIn(cluster)
      LOG.debug(s"Finished install $snapshot")
      snapshot.lastLogEntryIndex + 1
    } else {
      1 //no snapshot to reload. start from index #1
    }
  }

  private def replay(from: Int, to: Int) = {
    LOG.debug(s"Start log replay from index #$from to #$to")
    from to to foreach { index => replayIndex(index) }
    LOG.debug(s"Finished log replay")
  }

  private def replayIndex(index: Int) = {
    LOG.debug(s"Replaying index #$index")
    val logEntry = persistentLog.getEntry(index)
    afterAppend(logEntry)
    execute(logEntry.command)
  }

  def getSnapshot(): Option[Snapshot] = {
    val snapshots = cluster.db.getTreeMap[Long, Array[Byte]]("snapshots")
    val lastSnapshot = snapshots.lastEntry()
    if (lastSnapshot != null) Some(Snapshot.deserialize(lastSnapshot.getValue())) else None
  }

  def serializeStateMachine = stateMachine.serialize()

  private def raiseMissingLogEntryException(entryIndex: Int) = {
    val e = new IllegalStateException(s"Tried to commit a missing LogEntry with index $entryIndex. A Hole?")
    LOG.error("Error", e)
    throw e
  }

  private def shared[T](f: => T): T = {
    sharedLock.lock()
    try {
      f
    } finally {
      sharedLock.unlock()
    }
  }

  def exclusive[T](f: => T): T = {
    exclusiveLock.lock()
    try {
      f
    } finally {
      exclusiveLock.unlock()
    }
  }

}