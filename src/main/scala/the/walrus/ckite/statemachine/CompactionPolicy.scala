package the.walrus.ckite.statemachine

import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.RLog

trait CompactionPolicy {

  def execute(logEntry: LogEntry, rlog: RLog)
  
}