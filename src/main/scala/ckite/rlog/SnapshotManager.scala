package ckite.rlog

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

import com.twitter.concurrent.NamedPoolThreadFactory

import ckite.Configuration
import ckite.RLog
import ckite.rpc.CompactedEntry
import ckite.rpc.LogEntry
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.Logging

class SnapshotManager(rlog: RLog, configuration: Configuration) extends Logging {

  val compacting = new AtomicBoolean(false)
  val logCompactionExecutor = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    new NamedPoolThreadFactory("LogCompaction-worker", true))
  val logCompactionPolicy = new FixedSizeLogCompactionPolicy(configuration.logCompactionThreshold)

  val cluster = rlog.cluster
  val stateMachine = rlog.stateMachine

  //index - term
  val latestSnapshotCoordinates = new AtomicReference[(Long, Int)]((0, 0))

  def applyLogCompactionPolicy = {
    if (logCompactionPolicy.applies(rlog.persistentLog, rlog.stateMachine)) {
      asyncCompact
    }
  }

  def asyncCompact = {
    val wasCompacting = compacting.getAndSet(true)
    if (!wasCompacting) {
      logCompactionExecutor.execute(() => {
          LOG.debug(s"Log compaction is required")
          compact
          compacting.set(false)
      })
    }
  }

  private def compact = {
    val snapshot = takeSnapshot
    save(snapshot)
    rollLog(snapshot.lastLogEntryIndex)
    updateLatestSnapshotCoordinates(snapshot)
  }
  
  private def updateLatestSnapshotCoordinates(snapshot: Snapshot) = {
    latestSnapshotCoordinates.set((snapshot.lastLogEntryIndex, snapshot.lastLogEntryTerm))
  }

  private def save(snapshot: Snapshot) = {
    LOG.debug(s"Saving Snapshot $snapshot")

    snapshot.write(configuration.dataDir)

    LOG.debug(s"Finished saving Snapshot ${snapshot}")
  }

  //rolls the current log up to the given logIndex
  private def rollLog(logIndex: Long) = rlog.exclusive {
    rlog.persistentLog.rollLog(logIndex)
  }

  private def takeSnapshot: Snapshot = rlog.exclusive {
    // During compaction the following actions must be blocked: 1. add log entries  2. execute commands in the state machine
    val commitIndex = rlog.commitIndex
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

    LOG.debug(s"Finished installing $snapshot")
    true //?
  }

  def reloadSnapshot: Long = {
    latestSnapshot map { snapshot => 
      LOG.info(s"Reloading $snapshot")
      stateMachine.deserialize(ByteBuffer.wrap(snapshot.stateMachineBytes))
      snapshot.membership.recoverIn(cluster)
      latestSnapshotCoordinates.set((snapshot.lastLogEntryIndex, snapshot.lastLogEntryTerm))
      LOG.info(s"Finished reloading $snapshot")
      snapshot.lastLogEntryIndex + 1
    } getOrElse {
      1 //no snapshot to reload. start from index #1
    }
  }

  def reload(snapshot: Snapshot) = {
    LOG.info(s"Reloading $snapshot")
    stateMachine.deserialize(ByteBuffer.wrap(snapshot.stateMachineBytes))
    LOG.info("Restoring cluster configuration from Snapshot...")
    snapshot.membership.recoverIn(cluster)
    latestSnapshotCoordinates.set((snapshot.lastLogEntryIndex, snapshot.lastLogEntryTerm))
    LOG.info(s"Finished reloading $snapshot")
  }
  
  def latestSnapshot: Option[Snapshot] = {
     latestSnapshotFile map { snapshotFile =>
      Snapshot.read(snapshotFile)
    }
  }
  
  def latestSnapthotIndex = latestSnapshotCoordinates.get()._1
  
  private def latestSnapshotFile: Option[File] = { 
    val snapshotDir = Option(new File(s"${configuration.dataDir}/snapshots"))
    snapshotDir.filter(dir => dir.exists()).map {dir => dir.list().toList.filter( fileName => fileName.startsWith("snapshot"))
      .sorted.headOption.map( fileName => new File(s"${configuration.dataDir}/snapshots/$fileName")) }.flatten
  }

  def isInSnapshot(index: Long, term: Int): Boolean = {
    val coordinates = latestSnapshotCoordinates.get()
    coordinates._2 >= term && coordinates._1 >= index
  }

  def isInSnapshot(index: Long): Boolean = {
    val coordinates = latestSnapshotCoordinates.get()
    coordinates._1 >= index
  }

  def compactedEntry = {
    val coordinates = latestSnapshotCoordinates.get()
    LogEntry(coordinates._2, coordinates._1, CompactedEntry())
  }

}
