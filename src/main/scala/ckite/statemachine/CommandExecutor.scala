package ckite.statemachine

import ckite.rpc.Command
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand
import ckite.util.Logging

class CommandExecutor(stateMachine: StateMachine) extends Logging {

  val writeFunction = stateMachine.applyWrite
  val readFunction = stateMachine.applyRead

  def applyWrite(index: Long, write: WriteCommand): Any = {
    val params = (index, write)
    if (writeFunction.isDefinedAt(params)) writeFunction(params)
    else LOG.warn(s"No handler for ${write} is available in the StateMachine")
  }

  def applyRead(read: ReadCommand): Any = {
//    readFunction(read)
    if (readFunction.isDefinedAt(read)) readFunction(read)
    else LOG.warn(s"No handler for ${read} is available in the StateMachine")
  }

}