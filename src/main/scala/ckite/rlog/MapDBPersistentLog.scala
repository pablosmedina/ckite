package ckite.rlog

import ckite.rpc.LogEntry
import ckite.Cluster
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ThreadPoolExecutor
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import ckite.util.Logging
import org.mapdb.DB
import java.util.concurrent.atomic.AtomicLong

class MapDBPersistentLog(db: DB) extends PersistentLog with Logging {

  val entries = db.getTreeMap[Int, LogEntry]("logEntries")
  val cachedSize = new AtomicLong(entries.size())
  
  def append(logEntry: LogEntry) = {
    entries.put(logEntry.index, logEntry)
    cachedSize.incrementAndGet()
    db.commit()
  }

  def getEntry(index: Int): LogEntry = entries.get(index)
  
  def rollLog(upToIndex: Int) = {
    val range = firstIndex to upToIndex
    LOG.debug(s"Compacting ${range.size} LogEntries")
    range foreach { index => remove(index) }
    LOG.debug(s"Finished compaction")
  }

  def getLastIndex(): Int = if (entries.isEmpty) 0 else entries.keySet.last()

  def size() = cachedSize.intValue()
  
  def remove(index: Int) = {
    entries.remove(index)
    cachedSize.decrementAndGet()
  }
  
  private def firstIndex: Int = entries.firstKey()


}