package ckite.statemachine

import ckite.rpc.Command
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand

class CommandExecutor(stateMachine: StateMachine) {

  val writeFunction = stateMachine.applyWrite
  val readFunction = stateMachine.applyRead

  def applyWrite(index: Long, write: WriteCommand): Any = {
    val params = (index, write)
    if (writeFunction.isDefinedAt(params)) writeFunction(params)
  }

  def applyRead(read: ReadCommand): Any = {
    if (readFunction.isDefinedAt(read)) readFunction(read)
  }

}