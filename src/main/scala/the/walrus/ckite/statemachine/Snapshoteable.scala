package the.walrus.ckite.statemachine

trait Snapshoteable {

  def deserialize(snapshotBytes: Array[Byte])
  
  def serialize(): Array[Byte]
  
}