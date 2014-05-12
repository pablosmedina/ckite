package ckite.rpc

import com.esotericsoftware.kryo.Kryo
import ckite.util.KryoSerializer
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.KryoSerializable

case class JointConfiguration(var oldBindings: List[String], var newBindings: List[String]) extends WriteCommand with KryoSerializable with ClusterConfigurationCommand {
     def write(kryo: Kryo, output: Output) = {
       output.writeString(oldBindings.mkString(","))
       output.writeString(newBindings.mkString(","))
   }
   def read(kryo: Kryo, input: Input) = {
      oldBindings = input.readString().split(",").toList
      newBindings = input.readString().split(",").toList
   }
}