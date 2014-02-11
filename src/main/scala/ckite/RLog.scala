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
import ckite.rlog.FixedLogSizeCompactionPolicy
import ckite.rlog.Snapshot
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.concurrent.SynchronousQueue
import ckite.util.CKiteConversions._
import ckite.rpc.NoOp
import com.twitter.util.Future
import ckite.exception.NoMajorityReachedException
import ckite.rpc.CompactedEntry

class RLog(val cluster: Cluster, stateMachine: StateMachine, db: DB) extends Logging {

  val entries = db.getTreeMap[Int, LogEntry]("entries")
  val commitIndex = db.getAtomicInteger("commitIndex")

  val lastLog = new AtomicInteger(0)

  val lock = new ReentrantReadWriteLock()
  val exclusiveLock = lock.writeLock()
  val sharedLock = lock.readLock()

  val compactionPolicy = new FixedLogSizeCompactionPolicy(cluster.configuration.fixedLogSizeCompaction, db)

  val asyncApplierExecutor = new ThreadPoolExecutor(0, 2,
												    10L, TimeUnit.SECONDS,
												    new SynchronousQueue[Runnable](),
												    new NamedPoolThreadFactory("AsyncApplierWorker", true))

  replay()

  def tryAppend(appendEntries: AppendEntries) = {
    LOG.trace(s"Try appending $appendEntries")
    val canAppend = hasPreviousLogEntry(appendEntries)
    shared {
    	if (canAppend) appendWithLockAcquired(appendEntries.entries)
    	commitEntriesUntil(appendEntries.commitIndex)
    }
    applyLogCompactionPolicyAsync
    canAppend
  }

  def append(logEntries: List[LogEntry]) = {
    shared {
      appendWithLockAcquired(logEntries)
    }
    applyLogCompactionPolicyAsync
  }

  private def applyLogCompactionPolicyAsync = asyncApplierExecutor.execute(() => {
    cluster.inContext {
      compactionPolicy.apply(this)
    }
  })

  private def hasPreviousLogEntry(appendEntries: AppendEntries) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }

  private def appendWithLockAcquired(logEntries: List[LogEntry]) = {
    logEntries.foreach { logEntry =>
      if (!containsEntry(logEntry.index, logEntry.term)) {
    	  LOG.info(s"Appending $logEntry")
    	  entries.put(logEntry.index, logEntry)
    	  logEntry.command match {
	    	  case c: EnterJointConsensus => cluster.apply(c)
	    	  case c: LeaveJointConsensus => cluster.apply(c)
	    	  case _ => Unit
    	  }
      } else {
        LOG.warn(s"Discarding append of a duplicate entry $logEntry")
      }
    }
  }

  def commit(logEntry: LogEntry) = shared {
    val logEntryOption = getLogEntry(logEntry.index)
    if (logEntryOption.isDefined) {
      val entry = logEntryOption.get
      if (entry.term == cluster.local.term) {
        commitEntriesUntil(logEntry.index, true) //logEntry.index excluded
        safeCommit(logEntry.index)
      } else {
        LOG.warn(s"Unsafe to commit an old term entry: $entry")
      }
    } else raiseMissingLogEntryException(logEntry.index)
  }

  private def commitEntriesUntil(entryIndex: Int, exclusive: Boolean = false) = {
    val start = commitIndex.intValue() + 1
    val end = if (exclusive) entryIndex - 1 else entryIndex
    (start to end) foreach { index => safeCommit(index) }
  }

  private def safeCommit(entryIndex: Int) = {
    val logEntryOption = getLogEntry(entryIndex)
    if (logEntryOption.isDefined) {
      val entry = logEntryOption.get
      if (entryIndex > commitIndex.intValue()) {
        LOG.info(s"Committing $entry")
        commitIndex.set(entry.index)
        execute(entry.command)
      } else {
        LOG.info(s"Already committed entry $entry")
      }
    }
  }

  def execute(command: Command) = {
    LOG.info(s"Executing $command")
    shared {
      command match {
        case c: EnterJointConsensus => {
          asyncApplierExecutor.execute(() => {
            try  {
            	cluster.on(MajorityJointConsensus(c.newBindings))
            } catch {
              case e:NoMajorityReachedException => LOG.warn(s"Could not commit LeaveJointConsensus")
            }
          })
          true
        }
        case c: LeaveJointConsensus => true
        case c: NoOp => Unit
        case _ => stateMachine.apply(command)
      }
    }
  }

  def execute(command: ReadCommand) = stateMachine.apply(command)

  def getLogEntry(index: Int): Option[LogEntry] = {
    val entry = entries.get(index)
    if (entry != null) Some(entry) else None
  }

  def getLastLogEntry(): Option[LogEntry] = {
    val lastLogIndex = findLastLogIndex
    if (isInSnapshot(lastLogIndex)) return {
      val snapshot = getSnapshot().get
      return Some(LogEntry(snapshot.lastLogEntryTerm, snapshot.lastLogEntryIndex, CompactedEntry()))
    }
    getLogEntry(lastLogIndex)
  }

  def getPreviousLogEntry(logEntry: LogEntry): Option[LogEntry] = {
    getLogEntry(logEntry.index - 1)
  }

  def containsEntry(index: Int, term: Int) = {
    val logEntryOption = getLogEntry(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (isZeroEntry(index, term) || isInSnapshot(index, term))
  }

  private def isZeroEntry(index: Int, term: Int): Boolean = index == -1 && term == -1

  private def isInSnapshot(index: Int, term: Int): Boolean = shared {
    getSnapshot().map { snapshot => snapshot.lastLogEntryTerm >= term && snapshot.lastLogEntryIndex >= index }
      .getOrElse(false).asInstanceOf[Boolean]
  }
  
  private def isInSnapshot(index: Int): Boolean = shared {
    getSnapshot().map { snapshot =>  snapshot.lastLogEntryIndex >= index }
      .getOrElse(false).asInstanceOf[Boolean]
  }

  def resetLastLog() = lastLog.set(findLastLogIndex)

  def findLastLogIndex(): Int = {
    if (entries.isEmpty) return 0
    entries.keySet.last()
  }

  def getCommitIndex(): Int = {
    commitIndex.intValue()
  }

  def nextLogIndex() = {
    lastLog.incrementAndGet()
  }

  def size() = entries.size

  def installSnapshot(snapshot: Snapshot): Boolean = exclusive {
    LOG.info(s"Installing $snapshot")
    val snapshots = db.getTreeMap[Long, Array[Byte]]("snapshots")
    snapshots.put(System.currentTimeMillis(), snapshot.serialize)
    stateMachine.deserialize(snapshot.stateMachineState)
    commitIndex.set(snapshot.lastLogEntryIndex)
    snapshot.membership.recoverIn(cluster)
    LOG.info(s"Finished installing $snapshot")
    true
  }

  private def replay() = {
    val lastSnapshot = getSnapshot()
    var startIndex = 1
    if (lastSnapshot.isDefined) {
      val snapshot = lastSnapshot.get
      LOG.info(s"Installing $snapshot")
      startIndex = snapshot.lastLogEntryIndex + 1
      stateMachine.deserialize(snapshot.stateMachineState)
      snapshot.membership.recoverIn(cluster)
      LOG.info(s"Finished install $snapshot")
    }
    val currentCommitIndex = commitIndex.get()
    if (currentCommitIndex > 0 && startIndex <= currentCommitIndex) {
      LOG.info(s"Start log replay from index #$startIndex to #$commitIndex")
      startIndex to currentCommitIndex foreach { index =>
        LOG.info(s"Replaying index #$index")
        val logEntry = entries.get(index)
        logEntry.command match {
    	  case c: EnterJointConsensus => cluster.apply(c)
    	  case c: LeaveJointConsensus => cluster.apply(c)
    	  case _ => Unit
    	  }
        execute(logEntry.command)
      }
      LOG.info(s"Finished log replay")
    } else {
      LOG.info(s"No entries to be replayed")
    }
  }

  def getSnapshot(): Option[Snapshot] = {
    val snapshots = db.getTreeMap[Long, Array[Byte]]("snapshots")
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