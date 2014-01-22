package the.walrus.ckite.example

import the.walrus.ckite.statemachine.StateMachine
import java.util.concurrent.ConcurrentHashMap
import the.walrus.ckite.rpc.Command

class KVStore extends StateMachine {

  val map = new ConcurrentHashMap[String, String]()
  
  override def apply(command: Command): Any = {
		 command match {
		   case Put(key: String, value: String) =>  map.put(key, value)
		   case Get(key: String) =>  map.get(key)
		   case _ => ""
		 }
  }
  
    def deserialize(snapshotBytes: Array[Byte]) = {
      
    }
  
  def serialize(): Array[Byte] = {
    null.asInstanceOf[Array[Byte]]
  }
  
}