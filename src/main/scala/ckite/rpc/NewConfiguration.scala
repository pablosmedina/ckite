package ckite.rpc

import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input

case class NewConfiguration (var bindings: List[String]) extends WriteCommand[Boolean] with KryoSerializable with ClusterConfigurationCommand {
     def write(kryo: Kryo, output: Output) = {
       output.writeString(bindings.mkString(","))
   }
   
   def read(kryo: Kryo, input: Input) = {
      bindings = input.readString().split(",").toList
   }
}