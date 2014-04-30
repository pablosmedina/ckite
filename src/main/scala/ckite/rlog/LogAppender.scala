package ckite.rlog

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.util.CKiteConversions._
import ckite.rpc.WriteCommand
import scala.concurrent.Promise
import ckite.rpc.LogEntry
import ckite.RLog
import ckite.rpc.LogEntry
import scala.collection.mutable.ArrayBuffer
import ckite.util.Logging

class LogAppender(rlog: RLog, log: PersistentLog) extends Logging {

  val queue = new LinkedBlockingQueue[Append[_]]()
  var pendingFlushes = ArrayBuffer[(LogEntry, Append[_])]()
  val flushSize = rlog.cluster.configuration.flushSize
  val syncEnabled = rlog.cluster.configuration.syncEnabled

  val asyncPool = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("LogAppender-worker", true))
  val asyncExecutionContext = ExecutionContext.fromExecutor(asyncPool)

  def start = asyncExecutionContext.execute(asyncAppend _)
  
  def stop =  {
    asyncPool.shutdownNow()
    asyncPool.awaitTermination(10, TimeUnit.SECONDS)
  }

  //leader append
  def append(term: Int, write: WriteCommand): Promise[(LogEntry, Promise[Any])] = append(LeaderAppend(term, write))

  //follower append
  def append(entry: LogEntry): Promise[Long] = append(FollowerAppend(entry))

  private def append[T](append: Append[T]): Promise[T] = {
    val promise: Promise[T] = append.promise
    queue.offer(append)
    promise
  }

  private def asyncAppend = {
    try {
      while (true) {
        val append = next

        val logEntry = append.logEntry

        rlog.shared {
          log.append(logEntry)
        }

        pendingFlushes = pendingFlushes :+ (logEntry, append)
      }
    } catch {
      case e: InterruptedException => LOG.info("Shutdown LogAppender...")
    }
  }

  private def next: Append[_] = {
    if (queue.isEmpty()) {
      flush
      queue.take()
    } else {
      flushIfNecessary
      queue.poll()
    }
  }

  private def flush = {
    if (syncEnabled) log.commit
    pendingFlushes.foreach { pendingFlush =>
      val logEntry = pendingFlush._1
      val append = pendingFlush._2
      append.onFlush(logEntry)
    }
    pendingFlushes.clear
  }

  private def flushIfNecessary = if (pendingFlushes.length > flushSize) flush

  trait Append[T] {
    def promise: Promise[T]
    def logEntry: LogEntry
    def onFlush(logEntry: LogEntry)
  }

  case class LeaderAppend(term: Int, write: WriteCommand) extends Append[(LogEntry, Promise[Any])] {
    val _promise = Promise[(LogEntry, Promise[Any])]()
    val _valuePromise = Promise[Any]()
    def promise = _promise
    def logEntry = {
      val logEntry = LogEntry(term, rlog.nextLogIndex, write)
      rlog.applyPromises.put(logEntry.index, _valuePromise)
      logEntry
    }
    def onFlush(logEntry: LogEntry) = _promise.success((logEntry, _valuePromise))
  }

  case class FollowerAppend(entry: LogEntry) extends Append[Long] {
    val _promise = Promise[Long]()
    def promise = _promise
    def logEntry = entry
    def onFlush(logEntry: LogEntry) = _promise.success(logEntry.index)
  }

}