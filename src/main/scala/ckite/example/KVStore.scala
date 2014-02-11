package ckite.example

import java.util.concurrent.ConcurrentHashMap

import ckite.rpc.Command
import ckite.statemachine.StateMachine
import ckite.util.Serializer

class KVStore extends StateMachine {

  val map = new ConcurrentHashMap[String, String]()

  override def apply(command: Command): Any = command match {
      case Put(key: String, value: String) => { 
        map.put(key, value)
        value
      }
      case Get(key: String) => map.get(key)
  }

  def deserialize(snapshotBytes: Array[Byte]) = {
	  val deserializedMap:ConcurrentHashMap[String, String] = Serializer.deserialize[ConcurrentHashMap[String, String]](snapshotBytes)
	  map.clear()
	  map.putAll(deserializedMap)
  }

  def serialize(): Array[Byte] = Serializer.serialize(map)

}