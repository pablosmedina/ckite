package ckite.rlog

import ckite.statemachine.StateMachine

trait LogCompactionPolicy {

  def applies(persistentLog: Log, stateMachine: StateMachine): Boolean

}