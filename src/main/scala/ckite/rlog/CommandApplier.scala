package ckite.rlog

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.util.CKiteConversions._
import ckite.RLog
import ckite.statemachine.StateMachine
import ckite.util.Logging
import ckite.rpc.Command
import ckite.rpc.EnterJointConsensus
import scala.concurrent._
import scala.concurrent.duration._
import ckite.rpc.MajorityJointConsensus
import ckite.exception.WriteTimeoutException
import ckite.rpc.NoOp
import ckite.rpc.Void
import scala.util.Success
import ckite.rpc.LeaveJointConsensus
import ckite.rpc.ReadCommand
import scala.util.Try
import ckite.rpc.LogEntry

class CommandApplier(rlog: RLog, stateMachine: StateMachine) extends Logging {

  val applyPartialFunction = stateMachine.apply
  val commitIndexQueue = new LinkedBlockingQueue[Long]()

  val asyncPool = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("Async-worker", true))
  val asyncExecutionContext = ExecutionContext.fromExecutor(asyncPool)
  val workerPool = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("CommandApplier-worker", true))
  val workerExecutor = ExecutionContext.fromExecutor(workerPool)

  @volatile
  var commitIndex: Long = 0
  @volatile
  var lastApplied: Long = 0

  def start(index: Long) = {
    lastApplied = index
    workerExecutor.execute(asyncApplier _)
  }
  
  def stop = {
    workerPool.shutdown()
  }
  
  def commit(index: Long) = {
    if (lastApplied < index) commitIndexQueue.offer(index)
  }
  
  def applyRead(command: ReadCommand) = applyPartialFunction.apply(command)

  private def asyncApplier = {
    LOG.info(s"Starting applier from index #{}",lastApplied)
    while (true) {
      val index = next
      if (lastApplied < index) {
        val entry = rlog.logEntry(index)
        if (isFromCurrentTerm(entry)) {
          applyUntil(entry.get)
        }
      }
    }
  }
  
  private def isFromCurrentTerm(entryOption: Option[LogEntry]) = {
    entryOption.map(entry => entry.term == rlog.cluster.local.term).getOrElse(false)
  }

  private def applyUntil(entry: LogEntry) = rlog.shared {
    (lastApplied + 1) to entry.index foreach { index =>
      val entryToApply = if (index == entry.index) Some(entry) else rlog.logEntry(index)
      entryToApply.map { entry =>
        commitIndex = index
        LOG.debug("New commitIndex #{}", index)
        val command = entry.command
        LOG.debug("Will apply committed entry {}", entry)
        val result = execute(entry.command)
        lastApplied = index //What do we assume about the StateMachine persistence?
        LOG.debug("Last applied index is #{}", lastApplied)
        notifyResult(index, result)
      }.orElse {
        LOG.error(s"Missing index #$index")
        None
      }
    }
  }

  private def notifyResult(index: Long, result: Any) = {
    val applyPromise = rlog.applyPromises.get(index)
    if (applyPromise != null) {
      applyPromise.success(result)
      rlog.applyPromises.remove(index)
    }
  }

  private def isCommitted(index: Long) = index <= commitIndex

  private def execute(command: Command)(implicit context: ExecutionContext = asyncExecutionContext) = {
    LOG.debug("Executing {}", command)
    command match {
      case c: EnterJointConsensus => executeEnterJointConsensus(c)
      case c: LeaveJointConsensus => true
      case c: NoOp => true
      case _ => applyCommand(command)
    }
  }

  private def executeEnterJointConsensus(c: EnterJointConsensus)(implicit context: ExecutionContext = asyncExecutionContext) = {
    future {
      try {
        rlog.cluster.on(MajorityJointConsensus(c.newBindings))
      } catch {
        case e: WriteTimeoutException => LOG.warn(s"Could not commit LeaveJointConsensus")
      }
    }
    true
  }

  private def applyCommand(command: Command) = applyPartialFunction.apply(command)

  private def apply(command: Command): Any = {
    if (applyPartialFunction.isDefinedAt(command)) applyPartialFunction(command)
    else throw new UnsupportedOperationException("No command handler in StateMachine")
  }

  private def next = {
    if (commitIndexQueue.isEmpty()) {
    	commitIndexQueue.take()
    } else {
      commitIndexQueue.poll
    }
  }

}