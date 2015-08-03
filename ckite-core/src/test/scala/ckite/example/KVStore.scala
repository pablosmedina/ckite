package ckite.example

import java.nio.ByteBuffer
import java.util.HashMap

import ckite.statemachine.StateMachine
import ckite.util.{ Logging, Serializer }

class KVStore extends StateMachine with Logging {

  private var map = new HashMap[String, String]()
  private var lastIndex: Long = 0

  def applyWrite = {
    case (index, Put(key: String, value: String)) ⇒ {
      logger.debug(s"Put $key=$value")
      map.put(key, value)
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

  def getLastAppliedIndex: Long = lastIndex

  def restoreSnapshot(byteBuffer: ByteBuffer) = {
    map = Serializer.deserialize(byteBuffer.array())
  }

  def takeSnapshot(): ByteBuffer = {
    ByteBuffer.wrap(Serializer.serialize(map))
  }

}