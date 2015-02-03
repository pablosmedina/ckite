package ckite.rlog

import java.util.concurrent.{ LinkedBlockingQueue, SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import ckite.RLog
import ckite.rpc.{ ClusterConfigurationCommand, Command, LogEntry, WriteCommand }
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.{ CustomThreadFactory, Logging }

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future, Promise }

class LogAppender(rlog: RLog, localLog: Log) extends Logging {

}