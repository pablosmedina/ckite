package the.walrus.ckite.rlog
import the.walrus.ckite.RLog

trait CompactionPolicy {

  def apply(rlog: RLog)
  
}