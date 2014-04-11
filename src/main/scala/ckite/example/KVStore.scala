package ckite.example

import java.util.concurrent.ConcurrentHashMap
import ckite.rpc.Command
import ckite.statemachine.StateMachine
import ckite.util.Serializer
import java.util.concurrent.atomic.AtomicLong

class KVStore extends StateMachine {

  val map = new ConcurrentHashMap[String, String]()
  
  def onGet(key: String) = map.get(key)
  
  def onPut(key: String, value: String) = {
        map.put(key, value)
        value
  }
  
  def deserialize(snapshotBytes: Array[Byte]) = {
	  val deserializedMap:ConcurrentHashMap[String, String] = Serializer.deserialize[ConcurrentHashMap[String, String]](snapshotBytes)
	  map.clear()
	  map.putAll(deserializedMap)
  }

  def serialize(): Array[Byte] = Serializer.serialize(map)

}