package ckite.rlog

import ckite.statemachine.StateMachine
import ckite.util.Logging

class FixedSizeLogCompactionPolicy(fixedSize: Long) extends LogCompactionPolicy with Logging {

  def applies(persistentLog: Log, stateMachine: StateMachine) = {
    val size = persistentLog.size
    val applies = size >= fixedSize
    if (applies) {
      logger.info(s"Log size is ${size} and exceeds the maximum threshold of ${fixedSize}")
    }
    applies
  }

}