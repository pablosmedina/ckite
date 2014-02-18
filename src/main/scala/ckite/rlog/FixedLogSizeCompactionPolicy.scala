package ckite.rlog
import ckite.RLog
import ckite.util.Logging
import scala.collection.JavaConverters._
import org.mapdb.DB
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ThreadPoolExecutor
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import ckite.util.CKiteConversions._

class FixedLogSizeCompactionPolicy(logSize: Long, db: DB) extends CompactionPolicy with Logging {

  val logCompactor = new LogCompactor()
  val compacting = new AtomicBoolean(false)
  val logCompactionExecutor = new ThreadPoolExecutor(0, 1,
												    10L, TimeUnit.SECONDS,
												    new SynchronousQueue[Runnable](),
												    new NamedPoolThreadFactory("LogCompactionWorker", true))
  def apply(rlog: RLog) = {
    if (rlog.size >= logSize) {
      val wasCompacting = compacting.getAndSet(true)
      if (!wasCompacting) {
          logCompactionExecutor.execute(() => {
              rlog.cluster.inContext {
            	  LOG.debug(s"Log compaction is required")
            	  logCompactor.compact(rlog, db)
            	  compacting.set(false)
              }
          })
      }
    }
  }

}