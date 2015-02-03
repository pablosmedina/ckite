package ckite.example

import java.util.concurrent.ConcurrentHashMap
import java.nio.ByteBuffer
import ckite.statemachine.StateMachine
import ckite.util.Serializer
import ckite.util.Logging

class KVStore extends StateMachine with Logging {

  val map = new ConcurrentHashMap[String, String]()
  @volatile
  var lastIndex: Long = 0

  def applyWrite = {
    case (index, Put(key: String, value: String)) ⇒ {
      logger.debug(s"Put $key=$value")
      map.put(key, value);
      lastIndex = index
      value
    }
  }

  def applyRead = {
    case Get(key) ⇒ {
      logger.debug(s"Get $key")
      map.get(key)
    }
  }

  def lastAppliedIndex: Long = lastIndex

  def deserialize(byteBuffer: ByteBuffer) = {
    val snapshotBytes = byteBuffer.array()
    val deserializedMap: ConcurrentHashMap[String, String] = Serializer.deserialize[ConcurrentHashMap[String, String]](snapshotBytes)
    map.clear()
    map.putAll(deserializedMap)
  }

  def serialize(): ByteBuffer = ByteBuffer.wrap(Serializer.serialize(map))

}