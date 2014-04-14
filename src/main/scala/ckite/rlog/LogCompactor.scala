package ckite.rlog

import ckite.RLog
import ckite.util.Logging
import java.io.FileOutputStream
import ckite.rpc.LogEntry
import org.mapdb.DB
import ckite.Member
import ckite.MembershipState
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.util.CKiteConversions._
import java.io.OutputStream
import java.io.File

class LogCompactor(rlog: RLog, dataDir: String) extends Logging {

 
}

