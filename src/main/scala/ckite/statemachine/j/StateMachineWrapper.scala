package ckite.statemachine.j

import java.nio.ByteBuffer
import ckite.rpc.Command


class StateMachineWrapper(jstateMachine: StateMachine) extends ckite.statemachine.StateMachine {

  def deserialize(byteBuffer: ByteBuffer) = jstateMachine.deserialize(byteBuffer)
  
  def serialize(): ByteBuffer = jstateMachine.serialize
  
  def apply: PartialFunction[Command,Any] = {
    case c:Command => jstateMachine.apply(c)
  }
  
}