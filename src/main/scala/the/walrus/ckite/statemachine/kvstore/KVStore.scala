package the.walrus.ckite.statemachine.kvstore

import the.walrus.ckite.rpc.Command
import the.walrus.ckite.statemachine.StateMachine
import java.util.concurrent.ConcurrentHashMap
import the.walrus.ckite.rpc.Put
import the.walrus.ckite.rpc.Get
import the.walrus.ckite.rpc.StringResult
import the.walrus.ckite.rpc.Result

class KVStore extends StateMachine {

  val map = new ConcurrentHashMap[String, String]()
  
  override def apply(command: Command): Result = {
		 command match {
		   case Put(key: String, value: String) =>  StringResult(map.put(key, value))
		   case Get(key: String) =>  StringResult(map.get(key))
		   case _ => StringResult("")
		 }
  }
  
}