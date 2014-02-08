package the.walrus.ckite.rlog

import the.walrus.ckite.MembershipState
import the.walrus.ckite.util.Serializer

class Snapshot(val stateMachineState: Array[Byte], val lastLogEntryIndex: Int, val lastLogEntryTerm: Int, val membership: MembershipState) extends Serializable {

  def serialize(): Array[Byte] = Serializer.serialize(this)
  
  override def toString(): String = s"Snapshot(lastLogEntryTerm=$lastLogEntryTerm,lastLogEntryIndex=$lastLogEntryIndex)"
  
}

object Snapshot {
  
    def deserialize(snapshotBytes: Array[Byte]): Snapshot =  Serializer.deserialize(snapshotBytes)
}

