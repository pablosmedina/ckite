package ckite.rpc

import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input

case class LeaveJointConsensus (var bindings: List[String]) extends WriteCommand with KryoSerializable {
     def write(kryo: Kryo, output: Output) = {
       output.writeString(bindings.mkString(","))
   }
   
   def read(kryo: Kryo, input: Input) = {
      bindings = input.readString().split(",").toList
   }
}