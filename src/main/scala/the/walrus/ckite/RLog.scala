package the.walrus.ckite

import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.rpc.Command
import java.util.concurrent.atomic.AtomicInteger
import the.walrus.ckite.util.Logging
import the.walrus.ckite.statemachine.StateMachine
import the.walrus.ckite.statemachine.kvstore.KVStore
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import the.walrus.ckite.rpc.AppendEntries

//TODO: make me persistent
object RLog extends Logging {

  val entries = TrieMap[Int, LogEntry]()
  val commitIndex = new AtomicInteger(0)
  val lastLog = new AtomicInteger(0)
  val stateMachine: StateMachine = new KVStore()

  def tryAppend(appendEntries: AppendEntries)(implicit cluster: Cluster) = {
    LOG.trace(s"try appending $appendEntries")
    val canAppend = hasPreviousLogEntry(appendEntries)
    if (canAppend) append(appendEntries.entries)
    commitUntil(appendEntries.commitIndex)
    canAppend
  }
  
  private def hasPreviousLogEntry(appendEntries: AppendEntries)(implicit cluster: Cluster) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }
  
  def append(logEntries: List[LogEntry])(implicit cluster: Cluster) = {
    logEntries.foreach { logEntry =>
      LOG.info(s"Appending log entry $logEntry")
      entries.put(logEntry.index, logEntry)
    }
  }

  def getLogEntry(index: Int): Option[LogEntry] = entries.get(index)
  
  def getLastLogEntry() = {
    getLogEntry(lastLog.intValue())
  }

  def getPreviousLogEntry(logEntry: LogEntry) = {
    getLogEntry(logEntry.index - 1)
  }

  def containsEntry(index: Int, term: Int) = {
    val logEntryOption = entries.get(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (index == -1 && term == -1)
  }

  def commit(logEntry: LogEntry)(implicit cluster: Cluster) = {
    val entryOption = entries.get(logEntry.index)
    if (entryOption.isDefined) {
      val entry = entryOption.get
    	if (entry.term == cluster.local.term) {
    		commitEntriesUntil(logEntry.index)
    		safeCommit(logEntry.index)
    	} else {
    		LOG.info(s"Unsafe to commit an old term entry: $entry")
    	}
    }
  }

  private def commitEntriesUntil(entryIndex: Int) = {
    (commitIndex.intValue() + 1) until entryIndex foreach { index =>
      if (entries.contains(index)) {
        safeCommit(index)
      }
    }
  }

 private def commitUntil(leaderCommitIndex: Int) = {
    if (leaderCommitIndex > commitIndex.intValue()) {
      (commitIndex.intValue() + 1) to leaderCommitIndex foreach { index => safeCommit(index) }
    }
  }

  private def safeCommit(entryIndex: Int) = {
    val entryOption = entries.get(entryIndex)
    if (entryOption.isDefined) {
      val entry = entryOption.get
    	if (entryIndex > commitIndex.intValue()) {
    		LOG.info(s"Commiting entry $entry")
    		execute(entry.command)
    		commitIndex.set(entry.index)
    	} else {
    		LOG.info(s"Already commited entry $entry")
    	}
    }
  }

  def execute(command: Command) = {
    LOG.info(s"Executing $command")
    stateMachine.apply(command)
  }

  def resetLastLog() = lastLog.set(findLastLogIndex)

  def findLastLogIndex(): Int = {
    if (entries.isEmpty) return 0
    val entriesSeq = entries.keySet
    val sortedEntries = SortedSet(entriesSeq.toSeq: _*)
    sortedEntries.lastKey
  }

  def getCommitIndex(): Int = {
    commitIndex.intValue()
  }

  def nextLogIndex() = {
    lastLog.incrementAndGet()
  }

  override def toString() = {
    s"Entries=${entries}"
  }

}