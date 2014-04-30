package ckite.statemachine.j

import java.nio.ByteBuffer
import ckite.rpc.Command
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand

trait StateMachine {

  def deserialize(byteBuffer: ByteBuffer)
  
  def serialize(): ByteBuffer
  
  def applyWrite(index:Long, write: WriteCommand):Any
  
  def applyRead(read: ReadCommand):Any
  
  def lastAppliedIndex: Long
  
}