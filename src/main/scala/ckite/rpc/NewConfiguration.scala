package ckite.rpc

import com.esotericsoftware.kryo.{ Kryo, KryoSerializable }
import com.esotericsoftware.kryo.io.{ Input, Output }

case class NewConfiguration(var bindings: Set[String]) extends ClusterConfigurationCommand with KryoSerializable {
  def write(kryo: Kryo, output: Output) = {
    output.writeString(bindings.mkString(","))
  }

  def read(kryo: Kryo, input: Input) = {
    bindings = input.readString().split(",").toSet
  }
}