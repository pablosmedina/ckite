package ckite.mapdb

import java.util.concurrent.atomic.AtomicLong

import ckite.rlog.Log
import ckite.rpc.LogEntry
import ckite.util.{Logging, Serializer}
import org.mapdb.DBMaker

import scala.concurrent.Future

case class MapDBPersistentLog(dataDir: String) extends Log with FileSupport with Logging {

  val logDB = DBMaker.newFileDB(file(dataDir, "ckite-mapdb-log")).mmapFileEnable().closeOnJvmShutdown().transactionDisable().cacheDisable().make()

  val entries = logDB.getTreeMap[Long, Array[Byte]]("logEntries")
  val cachedSize = new AtomicLong(entries.size())
  val lastIndex = new AtomicLong(if (entries.isEmpty) 0 else entries.lastKey())

  def append(entry: LogEntry): Future[Unit] = Future.successful {
    entries.put(entry.index, Serializer.serialize(entry))
    cachedSize.incrementAndGet()
    lastIndex.set(entry.index)
    commit
  }

  def getEntry(index: Long): LogEntry = {
    val bytes = entries.get(index)
    if (bytes != null) Serializer.deserialize(bytes) else null.asInstanceOf[LogEntry]
  }

  def rollLog(upToIndex: Long) = {
    val range = firstIndex to upToIndex
    logger.debug(s"Compacting ${range.size} LogEntries")
    range foreach { index ⇒ remove(index)}
    logger.debug(s"Finished compaction")
  }

  def getLastIndex: Long = lastIndex.longValue()

  def size = cachedSize.longValue()

  def discardEntriesFrom(index: Long) = {
    index to lastIndex.longValue() foreach { i ⇒
      remove(i)
    }
    lastIndex.set(index - 1)
  }

  def close() = logDB.close()

  private def commit = logDB.commit()


  private def firstIndex: Long = if (!entries.isEmpty) entries.firstKey else 1

  private def remove(index: Long) = {
    entries.remove(index)
    cachedSize.decrementAndGet()
  }
}