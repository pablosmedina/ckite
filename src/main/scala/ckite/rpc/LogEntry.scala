package ckite.rpc

import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input

case class LogEntry(var term: Int, var index: Long, var command: Command) extends KryoSerializable {

  def write(kryo: Kryo, output: Output) = {
    output.writeInt(term)
    output.writeLong(index)
    kryo.writeClassAndObject(output, command)
  }
  
  def read(kryo: Kryo, input: Input) = {
    term = input.readInt()
    index = input.readLong()
    command = kryo.readClassAndObject(input).asInstanceOf[Command]
  }
  
  override def toString = s"LogEntry(term=$term,index=$index,$command)"
}
