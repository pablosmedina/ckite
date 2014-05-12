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
import ckite.rpc.JointConfiguration
import scala.concurrent._
import scala.concurrent.duration._
import ckite.rpc.MajorityJointConsensus
import ckite.exception.WriteTimeoutException
import ckite.rpc.NoOp
import ckite.rpc.Void
import scala.util.Success
import ckite.rpc.NewConfiguration
import ckite.rpc.ReadCommand
import scala.util.Try
import ckite.rpc.LogEntry
import ckite.statemachine.CommandExecutor
import ckite.rpc.WriteCommand
import ckite.rpc.WriteCommand
import ckite.rpc.ClusterConfigurationCommand
import ckite.rpc.LogEntry

class CommandApplier(rlog: RLog, stateMachine: StateMachine) extends Logging {

  val commandExecutor = new CommandExecutor(stateMachine)
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
  var lastApplied: Long = stateMachine.lastAppliedIndex

  def start:Unit = {
    workerExecutor.execute(asyncApplier _)
  }
  
  def start(index: Long):Unit = {
    lastApplied = index
    start
  }
  
  def stop = {
    workerPool.shutdownNow()
    asyncPool.shutdown()
    workerPool.awaitTermination(10, TimeUnit.SECONDS)
    asyncPool.awaitTermination(10, TimeUnit.SECONDS)
  }
  
  def commit(index: Long) = if (lastApplied < index) commitIndexQueue.offer(index)
  
  private def asyncApplier = {
    LOG.info(s"Starting applier from index #{}", lastApplied)
    try {
      while (true) {
        val index = next
        if (lastApplied < index) {
          val entry = rlog.logEntry(index)
          if (isFromCurrentTerm(entry)) {
            applyUntil(entry.get)
          }
        }
      }
    } catch {
      case e: InterruptedException => LOG.info("Shutdown CommandApplier...")
    }
  }

  def replay: Unit = {
    val latestClusterConfigurationEntry = findLatestClusterConfiguration
    latestClusterConfigurationEntry foreach { entry =>
      LOG.info("Found cluster configuration in the log: {}", entry.command)
      rlog.cluster.apply(entry.index, entry.command.asInstanceOf[ClusterConfigurationCommand])
    }
    val from = lastApplied + 1
    val to = commitIndex
    if (from > to) {
      LOG.info("No entry to replay. commitIndex is #{}", commitIndex)
      return
    }
    replay(from, to)
  }
  
  private def findLatestClusterConfiguration: Option[LogEntry] = {
     rlog.findLastLogIndex to 1 by -1 find { index => 
      val logEntry = rlog.logEntry(index)
      if (!logEntry.isDefined) return None
      logEntry.collect {case LogEntry(term,entry,c:ClusterConfigurationCommand) => true}.getOrElse(false)
    } map { index => rlog.logEntry(index) } flatten
  }
  
  private def replay(from: Long, to: Long) = {
    LOG.debug("Start log replay from index #{} to #{}",from,to)
    rlog.logEntry(to).foreach {
      entry => 
        applyUntil(entry)
    }
    LOG.debug("Finished log replay")
  }

  private def isFromCurrentTerm(entryOption: Option[LogEntry]) = {
    entryOption.map(entry => entry.term == rlog.cluster.local.term).getOrElse(false)
  }

  private def applyUntil(entry: LogEntry) = rlog.shared {
    (lastApplied + 1) to entry.index foreach { index =>
      val entryToApply = if (index == entry.index) Some(entry) else rlog.logEntry(index)
      entryToApply.map { entry =>
        commitIndex = index
        LOG.debug("New commitIndex is #{}", index)
        val command = entry.command
        LOG.debug("Will apply committed entry {}", entry)
        val result = execute(entry.index, entry.command)
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

  private def execute(index: Long, command: Command)(implicit context: ExecutionContext = asyncExecutionContext) = {
    LOG.debug("Executing {}", command)
    command match {
      case c: JointConfiguration => executeEnterJointConsensus(index, c)
      case c: NewConfiguration => true
      case c: NoOp => true
      case w: WriteCommand => commandExecutor.applyWrite(index, w)
    }
  }

  private def executeEnterJointConsensus(index:Long, c: JointConfiguration)(implicit context: ExecutionContext = asyncExecutionContext) = {
    if (index >= rlog.cluster.membership.index) {
    	future {
    		try {
    			rlog.cluster.on(MajorityJointConsensus(c.newBindings))
    		} catch {
    		case e: WriteTimeoutException => LOG.warn(s"Could not commit LeaveJointConsensus")
    		}
    	}
    } else {
      LOG.info("Skipping old configuration {}",c)
    }
    true
  }

  def applyRead(read: ReadCommand) = commandExecutor.applyRead(read)
  
  private def next = {
    if (commitIndexQueue.isEmpty()) {
    	commitIndexQueue.take()
    } else {
      commitIndexQueue.poll
    }
  }

}