package ckite.rlog

import java.util.concurrent.{ LinkedBlockingQueue, SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import ckite.rpc.{ ClusterConfigurationCommand, Command, JointConfiguration, LogEntry, NewConfiguration, NoOp, ReadCommand, WriteCommand }
import ckite.statemachine.{ CommandExecutor, StateMachine }
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.{ CustomThreadFactory, Logging }
import ckite.{ Consensus, RLog }

import scala.concurrent.Promise

class CommandApplier(consensus: Consensus, log: RLog, stateMachine: StateMachine) extends Logging {

  private val commandExecutor = new CommandExecutor(stateMachine)
  private val commitIndexQueue = new LinkedBlockingQueue[Long]()

  private val applierWorker = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), CustomThreadFactory("CommandApplier-worker"))

  @volatile
  var commitIndex: Long = 0
  @volatile
  var lastApplied: Long = stateMachine.lastAppliedIndex

  def start: Unit = {
    applierWorker.execute(asyncApplier _)
  }

  def start(index: Long): Unit = {
    lastApplied = index
    start
  }

  def stop = {
    applierWorker.shutdownNow()
    applierWorker.awaitTermination(10, TimeUnit.SECONDS)
  }

  def commit(index: Long) = {
    if (lastApplied < index) commitIndexQueue.offer(index)
  }

  private def asyncApplier() = {
    logger.info(s"Starting applier from index #{}", lastApplied)
    try {
      while (true) {
        val index = next
        if (lastApplied < index) {
          val entry = log.entry(index)
          if (isFromCurrentTerm(entry)) {
            applyUntil(entry.get)
          }
        }
      }
    } catch {
      case e: InterruptedException ⇒ logger.info("Shutdown CommandApplier...")
    }
  }

  def replay: Unit = {
    val from = lastApplied + 1
    val to = commitIndex
    if (from > to) {
      logger.info("No entry to replay. commitIndex is #{}", commitIndex)
      return
    }
    replay(from, to)
  }

  private def replay(from: Long, to: Long) = {
    logger.debug("Start log replay from index #{} to #{}", from, to)
    log.entry(to).foreach {
      entry ⇒
        applyUntil(entry)
    }
    logger.debug("Finished log replay")
  }

  private def isFromCurrentTerm(entryOption: Option[LogEntry]) = {
    entryOption.map(entry ⇒ entry.term == consensus.term).getOrElse(false)
  }

  private def applyUntil(entry: LogEntry) = log.shared {
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
    lastApplied = index //What do we assume about the StateMachine persistence?
    logger.debug("Last applied index is #{}", index)
  }

  private def entryToApply(index: Long, entry: LogEntry) = {
    if (index == entry.index) Some(entry) else log.entry(index)
  }

  private def notifyResult(index: Long, result: Any) = {
    val applyPromise = log.applyPromises.get(index).asInstanceOf[Promise[Any]]
    if (applyPromise != null) {
      applyPromise.success(result)
      log.applyPromises.remove(index)
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

  def applyRead[T](read: ReadCommand[T]) = commandExecutor.applyRead(read)

  private def next = {
    if (commitIndexQueue.isEmpty()) {
      commitIndexQueue.take()
    } else {
      commitIndexQueue.poll
    }
  }

}