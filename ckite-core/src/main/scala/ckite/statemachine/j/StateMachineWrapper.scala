package ckite.statemachine.j

import java.nio.ByteBuffer
import ckite.rpc.Command
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand

class StateMachineWrapper(jstateMachine: StateMachine) extends ckite.statemachine.StateMachine {

  def restoreSnapshot(byteBuffer: ByteBuffer) = jstateMachine.deserialize(byteBuffer)

  def takeSnapshot(): ByteBuffer = jstateMachine.serialize

  def applyWrite: PartialFunction[(Long, WriteCommand[_]), Any] = {
    case (index, write) ⇒ jstateMachine.applyWrite(index, write)
  }

  def applyRead: PartialFunction[ReadCommand[_], Any] = {
    case read ⇒ jstateMachine.applyRead(read)
  }

  def getLastAppliedIndex: Long = jstateMachine.lastAppliedIndex
}