package the.walrus.ckite.rlog

import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.Serializable
import java.io.ObjectInputStream
import java.io.ByteArrayInputStream

class Snapshot(val stateMachineState: Array[Byte], val lastLogEntryIndex: Int, val lastLogEntryTerm: Int) extends Serializable {

  def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos =   new ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.flush()
    oos.close()
    baos.toByteArray()
  }
  
  override def toString(): String = s"Snapshot(lastLogEntryTerm=$lastLogEntryTerm,lastLogEntryIndex=$lastLogEntryIndex)"
  
}

object Snapshot {
  
    def deserialize(snapshotBytes: Array[Byte]): Snapshot = {
    	  val inputStream = new ObjectInputStream(new ByteArrayInputStream(snapshotBytes))
		  val snapshot = inputStream.readObject().asInstanceOf[Snapshot]
		  inputStream.close()
		  snapshot
    }
}

