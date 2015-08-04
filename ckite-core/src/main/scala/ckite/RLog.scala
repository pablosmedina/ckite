package ckite

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import ckite.rlog._
import ckite.rpc.LogEntry.{ Index, Term }
import ckite.rpc._
import ckite.statemachine.{ CommandExecutor, StateMachine }
import ckite.stats.LogStats
import ckite.util.CKiteConversions._
import ckite.util.{ CustomThreadFactory, Logging }

import scala.Option.option2Iterable
import scala.collection.immutable.NumericRange
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }

case class RLog(raft: Raft, stateMachine: StateMachine, storage: Storage, configuration: Configuration) extends Logging {

  val log = storage.log()

  private def consensus = raft.consensus

  private def membership = raft.membership

  private val _lastIndex = new AtomicLong(0)

  private val snapshotManager = SnapshotManager(membership, this, storage, configuration)

  private val logWorker = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), CustomThreadFactory("Log-worker"))

  private val appendsQueue = new LinkedBlockingQueue[Append[_]]()

  private val commandExecutor = new CommandExecutor(stateMachine)

  private val messageQueue = new LinkedBlockingQueue[Message]()

  @volatile
  var commitIndex: Long = 0
  @volatile
  var lastApplied: Long = stateMachine.getLastAppliedIndex

  private val applyPromises = new ConcurrentHashMap[Long, Promise[_]]()

  def bootstrap() = {
    assertEmptyLog()
    assertNoSnapshot()
  }

  //Leader append path
  def append[T](term: Term, write: WriteCommand[T]): Future[(LogEntry, Promise[T])] = {
    append(LeaderAppend[T](term, write))
  }

  //Follower append path
  def tryAppend(appendEntries: AppendEntries): Future[Boolean] = {
    logger.trace("Try appending {}", appendEntries)
    val canAppend = hasPreviousLogEntry(appendEntries)
    if (canAppend) {
      appendAll(appendEntries.entries) map { _ ⇒
        commit(appendEntries.commitIndex)
        canAppend
      }
    } else {
      logger.trace("Rejecting {}", appendEntries)
      Future.successful(canAppend)
    }
  }

  private def hasPreviousLogEntry(appendEntries: AppendEntries) = {
    containsEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)
  }

  //Follower appends all these entries and waits for them to be flushed to the persistentLog
  private def appendAll(entries: List[LogEntry]): Future[List[Index]] = {
    val appendPromises = entries.map { entry ⇒
      logger.trace(s"Try appending $entry")
      Some(append(FollowerAppend(entry)))
    }
    composeFutures(appendPromises)
  }

  private def composeFutures(appendPromises: List[Option[Future[Index]]]) = {
    val futures = for {
      append ← appendPromises
      future ← append
    } yield future
    Future.sequence(futures)
  }

  private def hasIndex(index: Long) = log.getLastIndex >= index

  def commit(index: Long) = {
    if (lastApplied < index) {
      messageQueue.offer(WriteCommitMessage(index))
    }
  }

  def execute[T](command: ReadCommand[T]) = applyRead(command)

  def entry(index: Long, allowCompactedEntry: Boolean = false): Option[LogEntry] = {
    val entry = log.getEntry(index)
    if (entry != null) Some(entry)
    else if (allowCompactedEntry && snapshotManager.isInSnapshot(index)) Some(snapshotManager.compactedEntry)
    else None
  }

  def lastEntry: Option[LogEntry] = {
    val lastLogIndex = findLastLogIndex()
    if (snapshotManager.isInSnapshot(lastLogIndex)) {
      Some(snapshotManager.compactedEntry)
    } else {
      entry(lastLogIndex)
    }
  }

  def getPreviousLogEntry(logEntry: LogEntry): Option[LogEntry] = entry(logEntry.index - 1, true)

  private def containsEntry(index: Long, term: Int) = {
    val logEntryOption = entry(index)
    if (logEntryOption.isDefined) logEntryOption.get.term == term else (isZeroEntry(index, term) || snapshotManager.isInSnapshot(index, term))
  }

  private def isZeroEntry(index: Long, term: Int): Boolean = index == -1 && term == -1

  def resetLastIndex() = _lastIndex.set(findLastLogIndex())

  private def findLastLogIndex(): Long = {
    val lastIndex = log.getLastIndex
    if (lastIndex > 0) lastIndex else snapshotManager.latestSnapshotIndex
  }

  private def nextLogIndex() = _lastIndex.incrementAndGet()

  def size() = log.size

  def stop() = {
    logWorker.shutdownNow()
    logWorker.awaitTermination(10, TimeUnit.SECONDS)
    log.close()
  }

  def serializeStateMachine = stateMachine.takeSnapshot()

  private def assertEmptyLog() = {
    if (log.size > 0) throw new IllegalStateException("Log is not empty")
  }

  private def assertNoSnapshot() = {
    if (snapshotManager.latestSnapshotIndex > 0) throw new IllegalStateException("A Snapshot was found")
  }

  def initialize() = {
    logger.info("Initializing RLog...")
    restoreLatestClusterConfiguration()
    replay()
    startLogWorker()
    logger.info("Done initializing RLog")
  }

  private def replay(): Unit = {
    lastApplied = reloadSnapshot()
    val from = lastApplied + 1
    val to = commitIndex
    if (from <= to) replay(from, to)
    else logger.info("No entry to replay. commitIndex is #{}", commitIndex)
  }

  private def reloadSnapshot(): Long = {
    val latestSnapshot = snapshotManager.latestSnapshot()
    val lastAppliedIndex: Long = latestSnapshot map { snapshot ⇒
      logger.info("Found a {}", snapshot)
      if (snapshot.index > lastApplied) {
        logger.info("The Snapshot has more recent data than the StateMachine. Will reload it...")
        snapshotManager.reload(snapshot)
        snapshot.index
      } else {
        logger.info("The StateMachine has more recent data than the Snapshot")
        membership.transitionTo(snapshot.clusterConfiguration)
        lastApplied
      }
    } getOrElse {
      logger.info("No Snapshot was found")
      0
    }
    lastAppliedIndex
  }

  def installSnapshot(snapshot: Snapshot) = {
    val promise = Promise[Unit]()
    messageQueue.offer(InstallSnapshotMessage(promise, snapshot))
    promise.future
  }

  def isInSnapshot(index: Index) = snapshotManager.isInSnapshot(index)

  def latestSnapshot() = snapshotManager.latestSnapshot()

  private def restoreLatestClusterConfiguration() = {
    val latestClusterConfigurationEntry = findLatestClusterConfiguration()
    latestClusterConfigurationEntry foreach { entry ⇒
      logger.info("Found cluster configuration in the log: {}", entry.command)
      consensus.membership.changeConfiguration(entry.index, entry.command.asInstanceOf[ClusterConfigurationCommand])
    }
  }

  private def findLatestClusterConfiguration(): Option[LogEntry] = {
    traversingInReversal find { index ⇒
      val logEntry = entry(index)
      if (!logEntry.isDefined) return None
      logEntry.collect { case LogEntry(term, entry, c: ClusterConfigurationCommand) ⇒ true }.getOrElse(false)
    } map { index ⇒ entry(index) } flatten
  }

  def traversingInReversal: NumericRange[Long] = {
    findLastLogIndex to 1 by -1
  }

  def rollLog(index: Long) = {
    log.rollLog(index)
  }

  def lastIndex(): Index = _lastIndex.longValue()

  def isEmpty: Boolean = lastIndex().equals(0L)

  private def startLogWorker() = logWorker.execute(runLogWorker _)

  private def append[T](append: Append[T]): Future[T] = {
    logger.trace(s"Append $append")
    val promise: Promise[T] = append.promise
    messageQueue.offer(AppendMessage(append))
    promise.future
  }

  private def runLogWorker() = {
    logger.info(s"Starting Log from index #{}", lastApplied)
    try {
      while (true) nextMessage()
    } catch {
      case e: InterruptedException ⇒ logger.info("Shutdown LogWorker...")
    }
  }

  private def applyLogCompactionPolicy() = snapshotManager.applyLogCompactionPolicy()

  private def onLogEntryAppended(append: Append[_])(entry: LogEntry) = {
    entry.command match {
      case configuration: ClusterConfigurationCommand ⇒ membership.changeConfiguration(entry.index, configuration)
      case _ ⇒ ;
    }
    append.onComplete(entry)
  }

  private def next: Append[_] = {
    if (appendsQueue.isEmpty()) {
      appendsQueue.take()
    } else {
      appendsQueue.poll()
    }
  }

  private def replay(from: Long, to: Long): Unit = {
    logger.debug("Start log replay from index #{} to #{}", from, to)
    entry(to).foreach {
      entry ⇒
        applyUntil(entry)
    }
    logger.debug("Finished log replay")
  }

  private def isFromCurrentTerm(entryOption: Option[LogEntry]) = {
    entryOption.exists(entry ⇒ entry.term.equals(consensus.term))
  }

  private def applyUntil(entry: LogEntry) = {
    (lastApplied + 1) to entry.index foreach { index ⇒
      entryToApply(index, entry).map { entry ⇒
        updateCommitIndex(index)
        logger.debug("Will apply committed entry {}", entry)
        val result = execute(entry.index, entry.command)
        updateLastAppliedIndex(index)
        notifyResult(index, result)
      }.orElse {
        logger.error(s"Missing index #$index")
        None
      }
    }
  }

  private def updateCommitIndex(index: Long) = {
    commitIndex = index
    logger.debug("New commitIndex is #{}", index)
  }

  private def updateLastAppliedIndex(index: Long) = {
    lastApplied = index //TODO: What do we assume about the StateMachine persistence?
    logger.debug("Last applied index is #{}", index)
  }

  private def entryToApply(index: Long, logEntry: LogEntry) = {
    if (index == logEntry.index) Some(logEntry) else entry(index)
  }

  private def notifyResult(index: Long, result: Any) = {
    val applyPromise = applyPromises.get(index).asInstanceOf[Promise[Any]]
    if (applyPromise != null) {
      applyPromise.success(result)
      applyPromises.remove(index)
    }
  }

  private def execute(index: Long, command: Command): Any = {
    command match {
      case jointConfiguration: JointConfiguration ⇒ consensus.onJointConfigurationCommitted(index, jointConfiguration)
      case newConfiguration: NewConfiguration     ⇒ consensus.onNewConfigurationCommitted(index, newConfiguration)
      case NoOp()                                 ⇒ true
      case write: WriteCommand[_]                 ⇒ executeInStateMachine(index, write)
    }
  }

  def executeInStateMachine(index: Long, write: WriteCommand[_]): Any = {
    logger.debug("Executing write {}", write)
    commandExecutor.applyWrite(index, write)
  }

  def applyRead[T](read: ReadCommand[T]) = {
    val promise = Promise[T]()
    messageQueue.offer(ReadApplyMessage(promise, read))
    promise.future
  }

  private def nextMessage = {
    if (messageQueue.isEmpty) {
      messageQueue.take()
    } else {
      messageQueue.poll
    }
  }

  def stats(): LogStats = LogStats(size(), commitIndex, lastEntry)

  trait Message {
    def apply()
  }

  case class WriteCommitMessage(index: Long) extends Message {
    def apply() = {
      if (lastApplied < index) {
        val logEntry = entry(index)
        if (isFromCurrentTerm(logEntry)) {
          applyUntil(logEntry.get)
        }
      }
    }
  }

  case class ReadApplyMessage[T](promise: Promise[T], read: ReadCommand[T]) extends Message {
    def apply() = promise.trySuccess(commandExecutor.applyRead(read))
  }

  case class InstallSnapshotMessage(promise: Promise[Unit], snapshot: Snapshot) extends Message {
    def apply() = snapshotManager.installSnapshot(snapshot)
  }

  case class AppendMessage[T](append: Append[T]) extends Message {
    def apply() = {
      val logEntry = append.logEntry

      logger.debug(s"Appending $logEntry")

      if (!containsEntry(logEntry.index, logEntry.term)) {
        if (hasIndex(logEntry.index)) {
          //If an entry is overridden then all the subsequent entries must be removed
          logger.debug("Will discard inconsistent entries starting from index #{} to follow Leader's log", logEntry.index)
          log.discardEntriesFrom(logEntry.index)
        }
        log.append(logEntry).map { _ ⇒
          onLogEntryAppended(append)(logEntry)
        }
        applyLogCompactionPolicy()
      } else {
        logger.debug("Discarding append of a duplicate entry {}", logEntry)
      }
    }
  }

  trait Append[T] {
    def promise: Promise[T]

    def logEntry: LogEntry

    def onComplete(logEntry: LogEntry)
  }

  case class LeaderAppend[T](term: Int, write: WriteCommand[T]) extends Append[(LogEntry, Promise[T])] {
    val _promise = Promise[(LogEntry, Promise[T])]()
    val _valuePromise = Promise[T]()

    def promise = _promise

    val logEntry = {
      val logEntry = LogEntry(term, nextLogIndex, write)
      applyPromises.put(logEntry.index, _valuePromise)
      logEntry
    }

    def onComplete(logEntry: LogEntry) = _promise.success((logEntry, _valuePromise))
  }

  case class FollowerAppend(entry: LogEntry) extends Append[Long] {
    val _promise = Promise[Long]()

    def promise = _promise

    def logEntry = entry

    def onComplete(logEntry: LogEntry) = _promise.success(logEntry.index)
  }

}