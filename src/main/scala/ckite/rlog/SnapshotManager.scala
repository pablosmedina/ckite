package ckite.rlog

import ckite.Configuration
import ckite.RLog
import ckite.util.Logging
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ThreadPoolExecutor
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import ckite.util.CKiteConversions._
import ckite.rpc.LogEntry
import ckite.rpc.CompactedEntry
import java.io.File

class SnapshotManager(rlog: RLog, configuration: Configuration) extends Logging {

  val compacting = new AtomicBoolean(false)
  val logCompactionExecutor = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    new NamedPoolThreadFactory("LogCompactionWorker", true))
  val logCompactionPolicy = new FixedSizeLogCompactionPolicy(configuration.logCompactionThreshold)

  val cluster = rlog.cluster
  val stateMachine = rlog.stateMachine

  //index - term
  val latestSnapshotCoordinates = new AtomicReference[(Int, Int)]((0, 0))

  def applyLogCompactionPolicy = {
    if (logCompactionPolicy.applies(rlog.persistentLog, rlog.stateMachine)) {
      asyncCompact
    }
  }

  def asyncCompact = {
    val wasCompacting = compacting.getAndSet(true)
    if (!wasCompacting) {
      logCompactionExecutor.execute(() => {
        rlog.cluster.inContext {
          LOG.debug(s"Log compaction is required")
          compact
          compacting.set(false)
        }
      })
    }
  }

  private def compact = {
    val snapshot = takeSnapshot
    persist(snapshot)
    rollLog(snapshot.lastLogEntryIndex)
    latestSnapshotCoordinates.set((snapshot.lastLogEntryIndex, snapshot.lastLogEntryTerm))
  }

  private def persist(snapshot: Snapshot) = {
    LOG.debug(s"Saving Snapshot $snapshot")

    snapshot.write(configuration.dataDir)

    LOG.debug(s"Finished saving Snapshot ${snapshot}")
  }

  //rolls the current log up to the given logIndex
  private def rollLog(logIndex: Int) = rlog.exclusive {
    rlog.persistentLog.rollLog(logIndex)
  }

  private def takeSnapshot: Snapshot = rlog.exclusive {
    // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
    val commitIndex = rlog.commitIndex.get()
    val membershipState = rlog.cluster.membership.captureState
    val latestEntry = rlog.logEntry(commitIndex).get
    val stateMachineBytes = rlog.serializeStateMachine

    new Snapshot(stateMachineBytes, latestEntry.index, latestEntry.term, membershipState)
  }

  def installSnapshot(snapshot: Snapshot): Boolean = rlog.exclusive {
    LOG.debug(s"Installing $snapshot")
    snapshot.write(configuration.dataDir)

    stateMachine.deserialize(ByteBuffer.wrap(snapshot.stateMachineBytes))
    snapshot.membership.recoverIn(cluster)

    rlog.commitIndex.set(snapshot.lastLogEntryIndex)

    LOG.debug(s"Finished installing $snapshot")
    true //?
  }

  def reloadSnapshot: Int = {
    latestSnapshot map { snapshot => 
      LOG.debug(s"Reloading $snapshot")
      stateMachine.deserialize(ByteBuffer.wrap(snapshot.stateMachineBytes))
      snapshot.membership.recoverIn(cluster)
      LOG.debug(s"Finished reloading $snapshot")
      snapshot.lastLogEntryIndex + 1
    } getOrElse {
      1 //no snapshot to reload. start from index #1
    }
  }
  
  def latestSnapshot: Option[Snapshot] = {
     latestSnapshotFile map { snapshotFile =>
      Snapshot.read(snapshotFile)
    }
  }
  
  private def latestSnapshotFile: Option[File] = { 
    val snapshotDir = Option(new File(s"${configuration.dataDir}/snapshots"))
    snapshotDir.filter(dir => dir.exists()).map {dir => dir.list().toList.filter( fileName => fileName.startsWith("snapshot"))
      .sorted.headOption.map( fileName => new File(fileName)) }.flatten
  }

  def isInSnapshot(index: Int, term: Int): Boolean = {
    val coordinates = latestSnapshotCoordinates.get()
    coordinates._2 >= term && coordinates._1 >= index
  }

  def isInSnapshot(index: Int): Boolean = {
    val coordinates = latestSnapshotCoordinates.get()
    coordinates._1 >= index
  }

  def compactedEntry = {
    val coordinates = latestSnapshotCoordinates.get()
    LogEntry(coordinates._2, coordinates._1, CompactedEntry())
  }

}
