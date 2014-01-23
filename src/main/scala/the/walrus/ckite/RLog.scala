package the.walrus.ckite

import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.rpc.WriteCommand
import java.util.concurrent.atomic.AtomicInteger
import the.walrus.ckite.util.Logging
import the.walrus.ckite.statemachine.StateMachine
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.LeaveJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.mapdb.DBMaker
import java.io.File
import org.mapdb.DB
import the.walrus.ckite.statemachine.FixedLogSizeCompactionPolicy

class RLog(val stateMachine: StateMachine, db: DB)(implicit cluster: Cluster) extends Logging {

  val entries = db.getTreeMap[Int, LogEntry]("entries")
  val commitIndex = db.getAtomicInteger("commitIndex")
  
  val lastLog = new AtomicInteger(0)
  val lock = new ReentrantReadWriteLock()
  val exclusiveLock = lock.writeLock()
  val sharedLock = lock.readLock()
  val compactionPolicy = new FixedLogSizeCompactionPolicy(5, cluster.configuration.dataDir)
  
  replay()
  
  private def replay() = {
    val ci = commitIndex.get()
    if (ci > 0) {
    	LOG.info(s"Start log replay. $ci LogEntries will be replayed")
    	1 to ci foreach { index => 
    	  LOG.info(s"Replaying index $index")
    	  execute(entries.get(index).command) 
    	}
    	LOG.info(s"Finished log replay")
    }
  }
  
  def tryAppend(appendEntries: AppendEntries)(implicit cluster: Cluster) = {
    sharedLock.lock()
    try {
      LOG.trace(s"Try appending $appendEntries")
      val canAppend = hasPreviousLogEntry(appendEntries)
      if (canAppend) appendWithLockAcquired(appendEntries.entries)
      commitUntil(appendEntries.commitIndex)
      canAppend
    } finally {
      sharedLock.unlock()
      compactionPolicy.apply(this)
    }
    
  }
  
  private def hasPreviousLogEntry(appendEntries: AppendEntries)(implicit cluster: Cluster) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }
  
  private def appendWithLockAcquired(logEntries: List[LogEntry])(implicit cluster: Cluster) = {
      logEntries.foreach { logEntry =>
      LOG.info(s"Appending log entry $logEntry")
      entries.put(logEntry.index, logEntry)
      logEntry.command match {
        case c: EnterJointConsensus => cluster.apply(c)
        case c: LeaveJointConsensus => cluster.apply(c)
        case _ => Unit
      } 
    }
  }
  
  def append(logEntries: List[LogEntry])(implicit cluster: Cluster) = {
	  	sharedLock.lock()
	  	try {
	  	  appendWithLockAcquired(logEntries)
	  	} finally {
	  	  sharedLock.unlock()
	  	  compactionPolicy.apply(this)
	  	}
  }

  def getLogEntry(index: Int): Option[LogEntry] = {
    val entry = entries.get(index)
    if (entry != null) Some(entry) else None
  }
  
  def getLastLogEntry() = {
    getLogEntry(findLastLogIndex)
  }

  def getPreviousLogEntry(logEntry: LogEntry) = {
    getLogEntry(logEntry.index - 1)
  }

  def containsEntry(index: Int, term: Int) = {
    val logEntryOption = getLogEntry(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (index == -1 && term == -1)
  }

  def commit(logEntry: LogEntry)(implicit cluster: Cluster) = {
    val logEntryOption = getLogEntry(logEntry.index)
    if (logEntryOption.isDefined) {
      val entry = logEntryOption.get
    	if (entry.term == cluster.local.term) {
    		commitEntriesUntil(logEntry.index)
    		safeCommit(logEntry.index)
    	} else {
    		LOG.info(s"Unsafe to commit an old term entry: $entry")
    	}
    }
  }

  private def commitEntriesUntil(entryIndex: Int)(implicit cluster: Cluster) = {
    (commitIndex.intValue() + 1) until entryIndex foreach { index =>
      if (entries.containsKey(index)) {
        safeCommit(index)
      }
    }
  }

 private def commitUntil(leaderCommitIndex: Int)(implicit cluster: Cluster) = {
    if (leaderCommitIndex > commitIndex.intValue()) {
      (commitIndex.intValue() + 1) to leaderCommitIndex foreach { index => safeCommit(index) }
    }
  }

  private def safeCommit(entryIndex: Int)(implicit cluster: Cluster) = {
     val logEntryOption = getLogEntry(entryIndex)
    if (logEntryOption.isDefined) {
      val entry = logEntryOption.get
    	if (entryIndex > commitIndex.intValue()) {
    		LOG.info(s"Commiting entry $entry")
    		commitIndex.set(entry.index)
    		execute(entry.command)
    	} else {
    		LOG.info(s"Already commited entry $entry")
    	}
    }
  }

  def execute(command: Command)(implicit cluster: Cluster) = {
    LOG.info(s"Executing $command")
    sharedLock.lock()
    try {
    	command match {
    	case c: EnterJointConsensus => cluster.on(MajorityJointConsensus(c.newBindings))
    	case c: LeaveJointConsensus => Unit
    	case _ => stateMachine.apply(command)
    	} 
    } finally {
      sharedLock.unlock()
    }
  }
  
  def execute(command: ReadCommand)(implicit cluster: Cluster) = {
    stateMachine.apply(command)
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

}