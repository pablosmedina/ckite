package ckite.rlog

import ckite.statemachine.StateMachine

trait LogCompactionPolicy {

  def applies(persistentLog: PersistentLog, stateMachine: StateMachine): Boolean
  
}