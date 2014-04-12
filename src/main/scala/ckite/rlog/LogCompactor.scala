package ckite.rlog

import ckite.RLog
import ckite.util.Logging
import java.io.FileOutputStream
import ckite.rpc.LogEntry
import org.mapdb.DB
import ckite.Member
import ckite.MembershipState
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.util.CKiteConversions._

class LogCompactor(rlog: RLog) extends Logging {

  val compacting = new AtomicBoolean(false)
  val logCompactionExecutor = new ThreadPoolExecutor(0, 1,
												    10L, TimeUnit.SECONDS,
												    new SynchronousQueue[Runnable](),
												    new NamedPoolThreadFactory("LogCompactionWorker", true))

  def asyncCompact() = {
    val wasCompacting = compacting.getAndSet(true)
    if (!wasCompacting) {
      logCompactionExecutor.execute(() => {
        rlog.cluster.inContext {
        	LOG.debug(s"Log compaction is required")
        	compact(rlog.cluster.db)
        	compacting.set(false)
        }
      })
    }
  }
  
  private def compact(db: DB) = {
    val capturedState = captureState()
    if (capturedState != null) {
      save(new Snapshot(capturedState._3, capturedState._2.index, capturedState._2.term, capturedState._4), db)
      rollLog(capturedState._1)
    }
  }

  private def save(snapshot: Snapshot, db: DB): Long = {
    val id = System.currentTimeMillis()
    LOG.debug(s"Saving Snapshot $snapshot")
    val snapshots = db.getTreeMap[Long, Array[Byte]]("snapshots")
    val ids = snapshots.keySet().toArray()
    snapshots.put(id, snapshot.serialize())
    ids.foreach { id =>
      snapshots.remove(id)
    }
    LOG.debug(s"Finished saving Snapshot $id")
    id
  }

  //rolls the current log up to the given logIndex
  private def rollLog(logIndex: Int) = rlog.exclusive {
    rlog.persistentLog.rollLog(logIndex)
  }

  private def captureState(): (Int, LogEntry, Array[Byte], MembershipState) = rlog.exclusive {
    // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
    val commitIndex = rlog.commitIndex.get()
    val membershipState = rlog.cluster.membership.captureState
    val latestEntry = rlog.logEntry(commitIndex).get
    val bytes = rlog.serializeStateMachine
    (commitIndex, latestEntry, bytes, membershipState)
  }
}

