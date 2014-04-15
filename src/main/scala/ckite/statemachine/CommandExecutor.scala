package ckite.statemachine

import ckite.rpc.Command

class CommandExecutor(stateMachine: StateMachine) {
  
  val applyPartialFunction = stateMachine.apply
  
  def apply(command: Command): Any = {
    if (applyPartialFunction.isDefinedAt(command)) applyPartialFunction(command) 
    else throw new UnsupportedOperationException("No command handler in StateMachine")
  }

}