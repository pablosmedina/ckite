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

/**
package ckite.util

import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayInputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.util.concurrent.ConcurrentLinkedQueue
import java.io.InputStream
import com.esotericsoftware.kryo.Kryo
import java.util.concurrent.atomic.AtomicLong
import com.esotericsoftware.kryo.io.UnsafeInput
import com.esotericsoftware.kryo.io.UnsafeOutput
import ckite.rpc.LogEntry
import org.objenesis.strategy.StdInstantiatorStrategy

object Serializer  {

  val kryoSerializer = new KryoSerializer(List())
  
	//TODO: make me more efficient
  def serialize[T](anObject: T): Array[Byte] = kryoSerializer.serialize(anObject)
  
  //TODO: make me more efficient
  def deserialize[T](bytes: Array[Byte]): T = kryoSerializer.deserialize(bytes)
  
}

class KryoSerializer(hintedClasses: List[Class[_]] = List.empty) extends Logging {

  val kryoFactory = new Factory[Kryo] {
    def newInstance(): Kryo = {
      val kryo = new Kryo()
      kryo.setReferences(false)
      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
      hintedClasses.foreach( hintedClass => kryo.register(hintedClass))
      kryo
    }
  }

  val pool = new KryoPool(kryoFactory, 10)

  def serialize[T](anObject: T): Array[Byte] = {
    val kryoHolder = pool.take()
    try {
      val kryo = kryoHolder
      val outputStream = new ByteArrayOutputStream()
      val output = new UnsafeOutput(outputStream)
      kryo.writeClassAndObject(output, anObject)
      output.flush()
      val bytes = outputStream.toByteArray()
      output.close()
      bytes
    } catch {
      case e: Exception => LOG.trace("error",e); throw e
    }  	finally {
      pool.release(kryoHolder)
    }
  }

  def deserialize[T](bytes: Array[Byte]): T = {
     deserialize(new UnsafeInput(bytes))
  }

  def deserialize[T](inputStream: InputStream): T = {
    deserialize(new UnsafeInput(inputStream))
  }

  def deserialize[T](input: UnsafeInput): T = {
    val kryo = pool.take()
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    }catch {
      case e: Exception => LOG.trace("error",e); throw e
    }  finally {
      pool.release(kryo)
    }
  }

}

trait Factory[T] {
  def newInstance(): T
}

class KryoPool(factory: Factory[Kryo], initInstances: Int)  {
  val instances = new AtomicLong(initInstances)
//  instances.addAndGet(initInstances)
  val maxInstances = initInstances * 2
  val objects = new ConcurrentLinkedQueue[Kryo]()

  (1 to initInstances) foreach { _ =>  objects.offer(factory.newInstance())}

  def take(): Kryo = {
    val pooledKryo = objects.poll()
    if (pooledKryo == null) {
      return factory.newInstance()
    }
    instances.decrementAndGet()
    return pooledKryo
  }

  def release(kh: Kryo) = {
    if (instances.intValue() < maxInstances) {
      instances.incrementAndGet()
      objects.offer(kh)
    }
  }

  def close() = {
    objects.clear()
  }


}


class KryoSerializerFactory {

  def create(hintedClasses: List[Class[_]]) = {
     new KryoSerializer(hintedClasses)
  }
}
*/