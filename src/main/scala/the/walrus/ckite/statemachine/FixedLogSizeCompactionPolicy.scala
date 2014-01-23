package the.walrus.ckite.statemachine

import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.RLog
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream
import the.walrus.ckite.util.Logging
import scala.collection.SortedSet
import scala.collection.JavaConverters._

class FixedLogSizeCompactionPolicy(logSize: Int, dataDir: String) extends CompactionPolicy with Logging {

  def apply(rlog: RLog) = {
    if (rlog.size >= logSize) {
      LOG.info(s"Log compaction is required")
      rlog.exclusiveLock.lock()
      // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
      val commitIndex = rlog.commitIndex.get()
      val latestEntry = rlog.getLogEntry(commitIndex).get
      val bytes = serializeStateMachineState(rlog)
      rlog.exclusiveLock.unlock()
      val snapshotId = saveSnapshot(new Snapshot(bytes, latestEntry.index, latestEntry.term))
      compact(rlog, commitIndex)
      //    	 deleteOldSnapshots(snapshotId)
    }
  }

  private def serializeStateMachineState(rlog: RLog): Array[Byte] = rlog.stateMachine.serialize()

  private def saveSnapshot(snapshot: Snapshot): Long = {
    val id = System.currentTimeMillis()
    LOG.info(s"Saving Snapshot $id")
    val fileOutputStream = new FileOutputStream(s"$dataDir/snapshot-$id")
    fileOutputStream.write(snapshot.serialize())
    fileOutputStream.flush()
    fileOutputStream.close()
    LOG.info(s"Finished saving Snapshot $id")
    id
  }

  //deletes all the snapshots in the snapshot dir except the new one
  private def deleteOldSnapshots(newSnapshotId: Long) = {

  }

  //rolls the current log up to the given logIndex
  private def compact(rlog: RLog, logIndex: Int) = {
    val firstIndex = rlog.entries.firstKey()
    val range = firstIndex to logIndex
    LOG.info(s"Compacting ${range.size} LogEntries")
    range foreach { index => rlog.entries.remove(index) }
    LOG.info(s"Finished compaction")
  }
}