package ckite.util

import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayInputStream
import java.io.ObjectOutputStream

object Serializer {

	//TODO: make me more efficient
  def serialize[T](anObject: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(anObject)
    oos.flush()
    baos.flush()
    val bytes = baos.toByteArray()
    oos.close()
    bytes
  }
  
  //TODO: make me more efficient
  def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[T]
    ois.close()
    deserialized
  }
  
}