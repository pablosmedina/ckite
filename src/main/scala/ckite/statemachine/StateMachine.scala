package ckite.statemachine

trait StateMachine {

  def deserialize(snapshotBytes: Array[Byte])
  
  def serialize(): Array[Byte]
  
}