package the.walrus.ckite.statemachine

import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.Serializable

class Snapshot(stateMachineState: Array[Byte], lastLogEntryIndex: Int, lastLogEntryTerm: Int) extends Serializable {

  def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos =   new ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.flush()
    oos.close()
    baos.toByteArray()
  }
  
}