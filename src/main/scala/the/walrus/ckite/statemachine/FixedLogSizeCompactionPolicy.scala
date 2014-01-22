package the.walrus.ckite.statemachine

import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.RLog
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream

class FixedLogSizeCompactionPolicy(logSize: Int) extends CompactionPolicy {

  def execute(logEntry: LogEntry, rlog: RLog) = {
     if (rlog.size >= logSize) {
         rlog.exclusiveLock.lock()
         // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
    	 val commitIndex = rlog.commitIndex.get()
    	 val latestEntry = rlog.getLogEntry(commitIndex).get
         val bytes = serializeStateMachineState(rlog)
         rlog.exclusiveLock.unlock()
    	 val snapshotId = saveSnapshot(new Snapshot(bytes, latestEntry.index, latestEntry.term))
    	 rollLog(rlog, commitIndex)
//    	 deleteOldSnapshots(snapshotId)
     }
  }
  
  private def serializeStateMachineState(rlog: RLog): Array[Byte] = rlog.stateMachine.serialize()
    
  
  private def saveSnapshot(snapshot: Snapshot): Long = {
    val id = System.currentTimeMillis()
    val fileOutputStream = new FileOutputStream(s"/tmp/snapshot-$id")
    fileOutputStream.write(snapshot.serialize())
    fileOutputStream.flush()
    fileOutputStream.close()
    id
  }
  
  //deletes all the snapshots in the snapshot dir except the new one
  private def deleteOldSnapshots(newSnapshotId: Long) = {
    
  }
  
  //rolls the current log up to the given logIndex
  private def rollLog(rlog: RLog, logIndex: Int) = {
    
  }
}