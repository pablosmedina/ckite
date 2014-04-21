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
import org.mapdb.DBMaker
import java.io.File
import ckite.rpc.WriteCommand
import ckite.rpc.LogEntry
import ckite.RLog
import ckite.util.Serializer

class MapDBPersistentLog(dataDir: String, rlog: RLog) extends PersistentLog with Logging {

  val logDB = DBMaker.newFileDB(file(dataDir)).mmapFileEnable().closeOnJvmShutdown().make()
  
  val entries = logDB.getTreeMap[Long, Array[Byte]]("logEntries")
  val cachedSize = new AtomicLong(entries.size())
  
  def append(term: Int, write: WriteCommand): LogEntry = {
    val logEntry = LogEntry(term, rlog.nextLogIndex, write)
    entries.put(logEntry.index, Serializer.serialize(logEntry))
    cachedSize.incrementAndGet()
    logEntry
  }
  
  def commit = {
	  if (cachedSize.longValue() % 1000 == 0) logDB.commit()
  }

  def append(entry: LogEntry): Unit = {
    entries.put(entry.index, Serializer.serialize(entry))
    cachedSize.incrementAndGet()
  }

  def getEntry(index: Long): LogEntry = { 
    val bytes = entries.get(index)
    if (bytes != null) Serializer.deserialize(bytes) else null.asInstanceOf[LogEntry]
  }
  
  def rollLog(upToIndex: Long) = {
    val range = firstIndex to upToIndex
    LOG.debug(s"Compacting ${range.size} LogEntries")
    range foreach { index => remove(index) }
    LOG.debug(s"Finished compaction")
  }

  def getLastIndex(): Long = if (entries.isEmpty) 0 else entries.keySet.last()

  def size() = cachedSize.longValue()
  
  def remove(index: Long) = {
    entries.remove(index)
    cachedSize.decrementAndGet()
  }
  
  def close() = {
    logDB.close()
  }
  
  private def firstIndex: Long = entries.firstKey()


  private def file(dataDir: String): File = {
    val dir = new File(dataDir)
    dir.mkdirs()
    val file = new File(dir, "rlog")
    file
  }
}