package ckite.rpc

import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import java.nio.ByteBuffer
import ckite.util.Serializer

case class LogEntry(val term: Int, val index: Long, val command: Command) {
  override def toString = s"LogEntry(term=$term,index=$index,$command)"
}

