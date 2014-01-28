package the.walrus.ckite.rlog

import the.walrus.ckite.RLog
import the.walrus.ckite.util.Logging
import java.io.FileOutputStream
import the.walrus.ckite.rpc.LogEntry
import org.mapdb.DB

class LogCompactor extends Logging {

  def compact(rlog: RLog, db: DB) = {
    val capturedState = captureState(rlog)
    if (capturedState != null) {
    	val snapshotId = save(new Snapshot(capturedState._3, capturedState._2.index, capturedState._2.term), db)
    	removeCompactedLogEntries(rlog, capturedState._1)
    }
  }

  private def save(snapshot: Snapshot, db: DB): Long = {
    val id = System.currentTimeMillis()
    LOG.info(s"Saving Snapshot $id")
    val snapshots = db.getTreeMap[Long,Array[Byte]]("snapshots")
    snapshots.put(id, snapshot.serialize())
    LOG.info(s"Finished saving Snapshot $id")
    id
  }

  //rolls the current log up to the given logIndex
  private def removeCompactedLogEntries(rlog: RLog, logIndex: Int) = {
    val firstIndex = rlog.entries.firstKey()
    val range = firstIndex to logIndex
    LOG.info(s"Compacting ${range.size} LogEntries")
    range foreach { index => rlog.entries.remove(index) }
    LOG.info(s"Finished compaction")
  }

  //deletes all the snapshots in the snapshot dir except the new one
  private def deleteOldSnapshots(newSnapshotId: Long) = {

  }

  private def captureState(rlog: RLog): (Int, LogEntry, Array[Byte]) = {
    // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
    rlog.exclusiveLock.lock()
    try {
        val commitIndex = rlog.commitIndex.get()
	    val latestEntry = rlog.getLogEntry(commitIndex).get
	    val bytes = rlog.serializeStateMachine
	    (commitIndex, latestEntry, bytes)
    } finally {
      rlog.exclusiveLock.unlock()
    }
  }
}

