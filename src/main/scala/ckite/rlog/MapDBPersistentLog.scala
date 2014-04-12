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

class MapDBPersistentLog(db: DB) extends PersistentLog with Logging {

  val entries = db.getTreeMap[Int, LogEntry]("logEntries")

  def append(logEntry: LogEntry) = entries.put(logEntry.index, logEntry)

  def getEntry(index: Int): LogEntry = entries.get(index)
  
  def rollLog(upToIndex: Int) = {
    val range = firstIndex to upToIndex
    LOG.debug(s"Compacting ${range.size} LogEntries")
    range foreach { index => remove(index) }
    LOG.debug(s"Finished compaction")
  }

  def getLastIndex(): Int = if (entries.isEmpty) 0 else entries.keySet.last()

  def size() = entries.size
  
  def remove(index: Int) = entries.remove(index)
  
  private def firstIndex: Int = entries.firstKey()


}