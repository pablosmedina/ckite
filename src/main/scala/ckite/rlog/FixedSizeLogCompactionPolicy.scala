package ckite.rlog

import ckite.statemachine.StateMachine

class FixedSizeLogCompactionPolicy(fixedSize: Long) extends LogCompactionPolicy {

  def applies(persistentLog: PersistentLog, stateMachine: StateMachine) = persistentLog.size >= fixedSize

}