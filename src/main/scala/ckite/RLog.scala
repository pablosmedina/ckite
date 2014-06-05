package ckite

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.Option.option2Iterable
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import ckite.rlog.CommandApplier
import ckite.rlog.LogAppender
import ckite.rlog.MapDBPersistentLog
import ckite.rlog.SnapshotManager
import ckite.rpc.AppendEntries
import ckite.rpc.Command
import ckite.rpc.JointConfiguration
import ckite.rpc.NewConfiguration
import ckite.rpc.LogEntry
import ckite.rpc.ReadCommand
import ckite.rpc.WriteCommand
import ckite.statemachine.StateMachine
import ckite.util.Logging
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


class RLog(val cluster: Cluster, val stateMachine: StateMachine) extends Logging {

  val persistentLog = new MapDBPersistentLog(cluster.configuration.dataDir, this)
  
  val lastLog = new AtomicLong(0)

  val lock = new ReentrantReadWriteLock()
  val exclusiveLock = lock.writeLock()
  val sharedLock = lock.readLock()

  val applyPromises = new ConcurrentHashMap[Long, Promise[_]]()

  val snapshotManager = new SnapshotManager(this, cluster.configuration)
  
  val logAppender = new LogAppender(this, persistentLog)
  val commandApplier = new CommandApplier(this, stateMachine)
  
  val appendPromiseTimeout = 3000 millis
  
  initialize()

  //Leader append
  def append[T](write: WriteCommand[T]): Future[(LogEntry, Promise[T])] = {
	val appendFuture = logAppender.append[T](cluster.local.term, write)
	applyLogCompactionPolicy
	appendFuture
  }

  //Follower append
  def tryAppend(appendEntries: AppendEntries): Future[Boolean] = {
    LOG.trace("Try appending {}", appendEntries)
    val canAppend = hasPreviousLogEntry(appendEntries)
    if (canAppend) {
      appendAll(appendEntries.entries) map { _ =>
        commandApplier.commit(appendEntries.commitIndex)
        applyLogCompactionPolicy
        canAppend
      }
    } else {
    	Promise.successful(canAppend).future
    }
  }

  private def applyLogCompactionPolicy = snapshotManager.applyLogCompactionPolicy

  private def hasPreviousLogEntry(appendEntries: AppendEntries) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }

  //Follower appends all these entries and waits for them to be flushed to the persistentLog
  private def appendAll(entries: List[LogEntry]) = {
    val appendPromises = entries.map { entry =>
      if (!containsEntry(entry.index, entry.term)) {
        if (hasIndex(entry.index)) {
        	//If an entry is overridden then all the subsequent entries must be removed
        	LOG.debug("Will discard inconsistent entries starting from index #{} to follow Leader's log",entry.index)
        	shared {
        		persistentLog.discardEntriesFrom(entry.index)
        	}
        }
        Some(logAppender.append(entry))
      } else {
        LOG.debug("Discarding append of a duplicate entry {}",entry)
        None
      }
    }
    waitForAll(appendPromises)
  }

  private def waitForAll(appendPromises: List[Option[Future[Long]]]) = {
    val futures = for {
       append <- appendPromises
       future <- append
    } yield future
    Future.sequence(futures)
  }
  
  private def hasIndex(index: Long) = persistentLog.getLastIndex >= index
  
  def commit(index: Long): Unit = commandApplier.commit(index)

  def commit(logEntry: LogEntry): Unit = commit(logEntry.index)

  def execute[T](command: ReadCommand[T]) = commandApplier.applyRead(command)

  def logEntry(index: Long, allowCompactedEntry: Boolean = false): Option[LogEntry] = {
    val entry = persistentLog.getEntry(index)
    if (entry != null) Some(entry)
    else if (allowCompactedEntry && snapshotManager.isInSnapshot(index)) Some(snapshotManager.compactedEntry)
    else None
  }

  private def withLogEntry[T](index: Long)(block: LogEntry => T) = logEntry(index) foreach block

  def getLastLogEntry(): Option[LogEntry] = {
    val lastLogIndex = findLastLogIndex
    if (snapshotManager.isInSnapshot(lastLogIndex)) {
      Some(snapshotManager.compactedEntry) 
    } else {
      logEntry(lastLogIndex)
    }
  }

  def getPreviousLogEntry(entry: LogEntry): Option[LogEntry] = logEntry(entry.index - 1, true)

  def containsEntry(index: Long, term: Int) = {
    val logEntryOption = logEntry(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (isZeroEntry(index, term) || snapshotManager.isInSnapshot(index, term))
  }

  private def isZeroEntry(index: Long, term: Int): Boolean = index == -1 && term == -1

  def resetLastLog() = lastLog.set(findLastLogIndex)

  def findLastLogIndex(): Long = {
    val lastIndex = persistentLog.getLastIndex
    if (lastIndex > 0) lastIndex else snapshotManager.latestSnapthotIndex
  }

  def commitIndex: Long = commandApplier.commitIndex

  def nextLogIndex() = lastLog.incrementAndGet()

  def size() = persistentLog.size

  def stop = {
    logAppender.stop
    commandApplier.stop
    persistentLog.close()
  }
  
  def serializeStateMachine = stateMachine.serialize().array()

  def assertEmptyLog = {
    if (persistentLog.size  > 0) throw new IllegalStateException("Log is not empty")
  }
  
  def assertNoSnapshot = {
    if (snapshotManager.latestSnapthotIndex > 0) throw new IllegalStateException("A Snapshot was found")
  }
  
  private def initialize() = {
    LOG.info("Initializing...")
    logAppender.start
    
    val latestSnapshot = snapshotManager.latestSnapshot
    val lastAppliedIndex:Long = latestSnapshot map { snapshot =>
      	LOG.info("Found a {}",snapshot)
    	if (snapshot.lastLogEntryIndex > commandApplier.lastApplied) {
    	  LOG.info("The Snapshot has more recent data than the StateMachine. Will reload it...")
    	  snapshotManager.reload(snapshot)
    	  snapshot.lastLogEntryIndex
    	} else {
    	  LOG.info("The StateMachine has more recent data than the Snapshot. Will just use the cluster configuration in the Snapshot...")
    	  snapshot.membership.recoverIn(cluster)
    	  commandApplier.lastApplied
    	}
    } getOrElse {
      LOG.info("No Snapshot was found")
      0
    }
    
    commandApplier.start(lastAppliedIndex)
    
    commandApplier.replay
  }

  private def raiseMissingLogEntryException(entryIndex: Long) = {
    val e = new IllegalStateException(s"Tried to commit a missing LogEntry with index $entryIndex. A Hole?")
    LOG.error("Error", e)
    throw e
  }

  def shared[T](f: => T): T = {
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