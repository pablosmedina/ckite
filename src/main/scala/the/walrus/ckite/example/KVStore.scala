package the.walrus.ckite.example

import the.walrus.ckite.statemachine.StateMachine
import java.util.concurrent.ConcurrentHashMap
import the.walrus.ckite.rpc.Command
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

class KVStore extends StateMachine with Serializable {

  val map = new ConcurrentHashMap[String, String]()

  override def apply(command: Command): Any = {
    command match {
      case Put(key: String, value: String) => map.put(key, value)
      case Get(key: String) => map.get(key)
      case _ => ""
    }
  }

  def deserialize(snapshotBytes: Array[Byte]) = {
	  val inputStream = new ObjectInputStream(new ByteArrayInputStream(snapshotBytes))
	  val deserializedMap = inputStream.readObject().asInstanceOf[ConcurrentHashMap[String, String]]
	  inputStream.close()
	  map.clear()
	  map.putAll(deserializedMap)
  }

  def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos =   new ObjectOutputStream(baos)
    oos.writeObject(map)
    oos.flush()
    oos.close()
    baos.toByteArray()
  }

}