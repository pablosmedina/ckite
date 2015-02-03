package ckite.rlog

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import ckite.rpc.LogEntry
import ckite.util.Logging

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.collection.JavaConverters._

case class MemoryStorage() extends Storage {

  private val volatileLog = new MemoryLog()
  private val latestSnapshot = new AtomicReference[Option[Snapshot]](None)
  private val latestVote = new AtomicReference[Option[Vote]](None)

  override def log(): Log = volatileLog

  override def retrieveLatestSnapshot(): Option[Snapshot] = latestSnapshot.get()

  override def saveVote(vote: Vote): Unit = latestVote.set(Some(vote))

  override def saveSnapshot(snapshot: Snapshot): Unit = latestSnapshot.set(Some(snapshot))

  override def retrieveLatestVote(): Option[Vote] = latestVote.get()
}

class MemoryLog extends Log with Logging {

  val map = new ConcurrentHashMap[Long, LogEntry]()

  override def append(entry: LogEntry): Future[Unit] = {
    map.put(entry.index, entry)
    Future.successful(())
  }

  override def rollLog(upToIndex: Long): Unit = {
    (1L to upToIndex) foreach { index ⇒
      logger.info(s"Removing entry #${index}")
      map.remove(index)
    }
  }

  override def size(): Long = map.size()

  override def getEntry(index: Long): LogEntry = map.get(index)

  override def discardEntriesFrom(index: Long): Unit = {
    discardEntriesFromRecursive(index)
  }

  @tailrec
  private def discardEntriesFromRecursive(index: Long): Unit = {
    if (map.remove(index) != null) discardEntriesFromRecursive(index + 1)
  }

  override def close(): Unit = {}

  override def getLastIndex(): Long = {
    if (size() > 0) map.keySet().asScala.toSeq.sorted.last else 0
  }

}
