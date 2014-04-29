package ckite.rpc

import com.esotericsoftware.kryo.Kryo
import ckite.util.KryoSerializer
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.KryoSerializable

case class EnterJointConsensus(var newBindings: List[String]) extends WriteCommand with KryoSerializable {
     def write(kryo: Kryo, output: Output) = {
       output.writeString(newBindings.mkString(","))
   }
   
   def read(kryo: Kryo, input: Input) = {
      newBindings = input.readString().split(",").toList
   }
}