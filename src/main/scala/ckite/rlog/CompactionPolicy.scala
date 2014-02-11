package ckite.rlog
import ckite.RLog

trait CompactionPolicy {

  def apply(rlog: RLog)
  
}