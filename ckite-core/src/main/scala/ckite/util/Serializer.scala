package ckite.util

import com.twitter.chill.ScalaKryoInstantiator

object Serializer {
  def serialize[T](anObject: T): Array[Byte] = KryoSerializer.serialize(anObject)
  def deserialize[T](bytes: Array[Byte]): T = KryoSerializer.deserialize(bytes)
}

object KryoSerializer {
  private val kryoPool = ScalaKryoInstantiator.defaultPool

  def serialize[T](anObject: T): Array[Byte] = kryoPool.toBytesWithClass(anObject)
  def deserialize[T](bytes: Array[Byte]): T = kryoPool.fromBytes(bytes).asInstanceOf[T]
}