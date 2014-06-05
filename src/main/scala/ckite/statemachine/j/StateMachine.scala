package ckite.statemachine.j

import java.nio.ByteBuffer
import ckite.rpc.Command
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand

trait StateMachine {

  def deserialize(byteBuffer: ByteBuffer)
  
  def serialize(): ByteBuffer
  
  def applyWrite(index:Long, write: WriteCommand[_]):Any
  
  def applyRead(read: ReadCommand[_]):Any
  
  def lastAppliedIndex: Long
  
}