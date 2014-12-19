package ckite.rlog

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import com.twitter.concurrent.NamedPoolThreadFactory

import ckite.RLog
import ckite.rpc.ClusterConfigurationCommand
import ckite.rpc.Command
import ckite.rpc.LogEntry
import ckite.rpc.WriteCommand
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.Logging

class LogAppender(rlog: RLog, persistentLog: PersistentLog) extends Logging {

  val queue = new LinkedBlockingQueue[Append[_]]()
  var pendingFlushes = ArrayBuffer[(LogEntry, Append[_])]()
  val flushSize = rlog.cluster.configuration.flushSize
  val syncEnabled = rlog.cluster.configuration.syncEnabled

  val asyncPool = new ThreadPoolExecutor(0, 1,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("LogAppender-worker", true))
  val asyncExecutionContext = ExecutionContext.fromExecutor(asyncPool)

  def start = asyncExecutionContext.execute(asyncAppend _)

  def stop = {
    asyncPool.shutdownNow()
    asyncPool.awaitTermination(10, TimeUnit.SECONDS)
  }

  //leader append
  def append[T](term: Int, write: WriteCommand[T]): Future[(LogEntry, Promise[T])] = append(LeaderAppend[T](term, write))

  //follower append
  def append(entry: LogEntry): Future[Long] = append(FollowerAppend(entry))

  private def append[T](append: Append[T]): Future[T] = {
    val promise: Promise[T] = append.promise
    queue.offer(append)
    promise.future
  }

  private def asyncAppend = {
    try {
      while (true) {
        val append = next

        val logEntry = append.logEntry

        rlog.shared {
          persistentLog.append(logEntry)
        }

        afterAppend(logEntry.index, logEntry.command)

        pendingFlushes = pendingFlushes :+ (logEntry, append)
      }
    } catch {
      case e: InterruptedException ⇒ log.info("Shutdown LogAppender...")
    }
  }

  private def afterAppend(index: Long, command: Command) = command match {
    case c: ClusterConfigurationCommand ⇒ rlog.cluster.apply(index, c)
    case _                              ⇒ ;
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
    if (syncEnabled) persistentLog.commit
    pendingFlushes.foreach { pendingFlush ⇒
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

  case class LeaderAppend[T](term: Int, write: WriteCommand[T]) extends Append[(LogEntry, Promise[T])] {
    val _promise = Promise[(LogEntry, Promise[T])]()
    val _valuePromise = Promise[T]()
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