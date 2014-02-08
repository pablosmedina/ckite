package the.walrus.ckite.rlog

import the.walrus.ckite.RLog
import the.walrus.ckite.util.Logging
import java.io.FileOutputStream
import the.walrus.ckite.rpc.LogEntry
import org.mapdb.DB
import the.walrus.ckite.Member
import the.walrus.ckite.MembershipState

class LogCompactor extends Logging {

  def compact(rlog: RLog, db: DB) = {
    val capturedState = captureState(rlog)
    if (capturedState != null) {
    	save(new Snapshot(capturedState._3, capturedState._2.index, capturedState._2.term, capturedState._4), db)
    	removeCompactedLogEntries(rlog, capturedState._1)
    }
  }

  private def save(snapshot: Snapshot, db: DB): Long = {
    val id = System.currentTimeMillis()
    LOG.info(s"Saving Snapshot $snapshot")
    val snapshots = db.getTreeMap[Long,Array[Byte]]("snapshots")
    val ids = snapshots.keySet().toArray()
    snapshots.put(id, snapshot.serialize())
    ids.foreach { id =>
      snapshots.remove(id)
    }
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

  private def captureState(rlog: RLog): (Int, LogEntry, Array[Byte],MembershipState) = rlog.exclusive {
    // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
    val commitIndex = rlog.commitIndex.get()
    val membershipState = rlog.cluster.membership.captureState
    val latestEntry = rlog.getLogEntry(commitIndex).get
    val bytes = rlog.serializeStateMachine
    (commitIndex, latestEntry, bytes, membershipState)
  }
}

