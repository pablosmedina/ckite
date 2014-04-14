package ckite.rlog

import ckite.MembershipState
import ckite.util.Serializer
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.File
import java.io.FileInputStream

class Snapshot(val stateMachineBytes: Array[Byte], val lastLogEntryIndex: Int, val lastLogEntryTerm: Int, val membership: MembershipState) extends Serializable {

  def write(dataDir: String) = {
    val outputStream = new FileOutputStream(snapshotFile(dataDir))
    
    outputStream.write(lastLogEntryIndex)
    outputStream.write(lastLogEntryTerm)
    val membershipBytes = Serializer.serialize(membership)
    outputStream.write(membershipBytes.length)
    outputStream.write(membershipBytes)
    outputStream.write(stateMachineBytes.length)
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
      val inputStream = new FileInputStream(snapshotFile)
       
      val lastLogEntryIndex = inputStream.read()
      val lastLogEntryTerm = inputStream.read()
      val membershipBytes = inputStream.read()
      val membership = new Array[Byte](membershipBytes)
      inputStream.read(membership)
      val stateMachineBytes = inputStream.read()
      val stateMachine = new Array[Byte](stateMachineBytes)
      
      inputStream.close()
      
      new Snapshot(stateMachine, lastLogEntryIndex, lastLogEntryTerm, Serializer.deserialize(membership))
    }
}

