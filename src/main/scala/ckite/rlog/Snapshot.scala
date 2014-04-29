package ckite.rlog

import ckite.MembershipState
import ckite.util.Serializer
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.DataOutputStream
import java.io.DataInputStream

class Snapshot(val stateMachineBytes: Array[Byte], val lastLogEntryIndex: Long, val lastLogEntryTerm: Int, val membership: MembershipState) extends Serializable {

  def write(dataDir: String) = {
    val outputStream = new DataOutputStream(new FileOutputStream(snapshotFile(dataDir)))
    
    outputStream.writeLong(lastLogEntryIndex)
    outputStream.writeInt(lastLogEntryTerm)
    val membershipBytes = Serializer.serialize(membership)
    outputStream.writeInt(membershipBytes.length)
    outputStream.write(membershipBytes)
    outputStream.writeInt(stateMachineBytes.length)
    outputStream.write(stateMachineBytes)
    
    outputStream.flush()
    outputStream.close()
  }
  
  private def snapshotFile(dataDir: String) = {
    val snapshotFile = new File(s"$dataDir/snapshots/snapshot-[${lastLogEntryIndex}-${lastLogEntryTerm}].bin")
    snapshotFile.getParentFile().mkdirs()
    snapshotFile.createNewFile()
    snapshotFile
  }

  override def toString(): String = s"Snapshot(lastLogEntryTerm=$lastLogEntryTerm,lastLogEntryIndex=$lastLogEntryIndex)"
  
}

object Snapshot {
  
    def read(snapshotFile: File): Snapshot =   {
      val inputStream = new DataInputStream(new FileInputStream(snapshotFile))
       
      val lastLogEntryIndex = inputStream.readLong()
      val lastLogEntryTerm = inputStream.readInt()
      val membershipBytes = inputStream.readInt()
      val membership = new Array[Byte](membershipBytes)
      inputStream.read(membership,0,membershipBytes)
      val stateMachineBytes = inputStream.readInt()
      val stateMachine = new Array[Byte](stateMachineBytes)
      inputStream.read(stateMachine,0,stateMachineBytes)
      inputStream.close()
      
      new Snapshot(stateMachine, lastLogEntryIndex, lastLogEntryTerm, Serializer.deserialize(membership))
    }
}

