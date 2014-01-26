package the.walrus.ckite.rlog
import the.walrus.ckite.RLog
import the.walrus.ckite.util.Logging
import scala.collection.JavaConverters._
import org.mapdb.DB

class FixedLogSizeCompactionPolicy(logSize: Long, db: DB) extends CompactionPolicy with Logging {

  val logCompactor = new LogCompactor()
  
  def apply(rlog: RLog) = {
    if (rlog.size >= logSize) {
      LOG.info(s"Log compaction is required")
      logCompactor.compact(rlog, db)
    }
  }

}