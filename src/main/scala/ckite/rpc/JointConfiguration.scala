package ckite.rpc

import com.esotericsoftware.kryo.{ Kryo, KryoSerializable }
import com.esotericsoftware.kryo.io.{ Input, Output }

case class JointConfiguration(var oldMembers: Set[String], var newMembers: Set[String]) extends ClusterConfigurationCommand with KryoSerializable {
  def write(kryo: Kryo, output: Output) = {
    output.writeString(oldMembers.mkString(","))
    output.writeString(newMembers.mkString(","))
  }

  def read(kryo: Kryo, input: Input) = {
    oldMembers = input.readString().split(",").toSet
    newMembers = input.readString().split(",").toSet
  }
}