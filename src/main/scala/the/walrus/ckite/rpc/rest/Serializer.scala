package the.walrus.ckite.rpc.rest

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

object Serializer {

  def toByteArray[T](anObject: T): Array[Byte] = {
		val baos = new ByteArrayOutputStream();
        val out = new ObjectOutputStream(baos);
        out.writeObject(anObject);
        out.flush();
        baos.toByteArray();
  }
  
  def fromByteArray[T](byteArray: Array[Byte]): T = {
    	fromInputStream(new ByteArrayInputStream(byteArray))
  }
  
  def fromInputStream[T](inputStream: InputStream): T = {
	   val objectInputStream = new ObjectInputStream(inputStream)
	   objectInputStream.readObject().asInstanceOf[T]
  }
  
}