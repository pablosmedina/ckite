package ckite.rlog

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import java.util.concurrent.{ Executors, SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import ckite.rpc.LogEntry.{ Index, Term }
import ckite.rpc.{ CompactedEntry, LogEntry }
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.{ CustomThreadFactory, Logging }
import ckite.{ Configuration, Membership, RLog }

import scala.concurrent.{ ExecutionContext, Future }

case class SnapshotManager(membership: Membership, rlog: RLog, storage: Storage, configuration: Configuration) extends Logging {

  val logCompactionPolicy = new FixedSizeLogCompactionPolicy(configuration.logCompactionThreshold)

  val stateMachine = rlog.stateMachine

  val latestSnapshotCoordinates = new AtomicReference[(Index, Term)]((0, 0))

  def applyLogCompactionPolicy() = {
    if (logCompactionPolicy.applies(rlog.log, rlog.stateMachine)) {
      logger.debug(s"Log compaction is required")
      compact()
    }
  }

  private def compact() = {
    val snapshot = takeSnapshot()
    save(snapshot)
    //rolls the log up to the given logIndex
    rlog.rollLog(snapshot.index)
    updateLatestSnapshotCoordinates(snapshot)
  }

  private def updateLatestSnapshotCoordinates(snapshot: Snapshot) = {
    latestSnapshotCoordinates.set((snapshot.index, snapshot.term))
  }

  private def save(snapshot: Snapshot) = {
    logger.debug(s"Saving Snapshot $snapshot")

    storage.saveSnapshot(snapshot)

    logger.debug(s"Finished saving Snapshot ${snapshot}")
  }

  private def takeSnapshot(): Snapshot = {
    val latestEntry = rlog.entry(rlog.lastApplied).get
    val clusterConfiguration = membership.clusterConfiguration
    val stateMachineSerialized = rlog.serializeStateMachine
    Snapshot(latestEntry.term, latestEntry.index, clusterConfiguration, stateMachineSerialized)
  }

  def installSnapshot(snapshot: Snapshot) = {
    logger.debug(s"Installing $snapshot")
    storage.saveSnapshot(snapshot)

    stateMachine.restoreSnapshot(snapshot.stateMachineSerialized)

    membership.transitionTo(snapshot.clusterConfiguration)

    logger.debug(s"Finished installing $snapshot")
  }

  def reload(snapshot: Snapshot) = {
    logger.info(s"Reloading $snapshot")
    stateMachine.restoreSnapshot(snapshot.stateMachineSerialized)
    membership.transitionTo(snapshot.clusterConfiguration)
    latestSnapshotCoordinates.set((snapshot.index, snapshot.term))
    logger.info(s"Finished reloading $snapshot")
  }

  def latestSnapshot(): Option[Snapshot] = {
    storage.retrieveLatestSnapshot()
  }

  def latestSnapshotIndex = latestSnapshotCoordinates.get()._1

  def isInSnapshot(index: Index, term: Term): Boolean = {
    val coordinates = latestSnapshotCoordinates.get()
    coordinates._2 >= term && coordinates._1 >= index
  }

  def isInSnapshot(index: Index): Boolean = {
    val coordinates = latestSnapshotCoordinates.get()
    coordinates._1 >= index
  }

  def compactedEntry = {
    val coordinates = latestSnapshotCoordinates.get()
    LogEntry(coordinates._2, coordinates._1, CompactedEntry())
  }

}