package ckite.statemachine

import ckite.rpc.Command
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand
import ckite.util.Logging

class CommandExecutor(stateMachine: StateMachine) extends Logging {

  val writeFunction = stateMachine.applyWrite
  val readFunction = stateMachine.applyRead

  def applyWrite[T](index: Long, write: WriteCommand[T]): T = {
    val params = (index, write)
    if (writeFunction.isDefinedAt(params)) writeFunction(params).asInstanceOf[T]
    else {
      log.warn(s"No handler for ${write} is available in the StateMachine")
      throw new IllegalStateException(s"No handler for ${write}")
    }
  }

  def applyRead[T](read: ReadCommand[T]): T = {
    if (readFunction.isDefinedAt(read)) readFunction(read).asInstanceOf[T]
    else {
      log.warn(s"No handler for ${read} is available in the StateMachine")
      throw new IllegalStateException(s"No handler for ${read}")
    }
  }

}