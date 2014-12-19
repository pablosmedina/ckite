package ckite

import org.scalatest.Matchers
import ckite.util.Logging
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import ckite.rpc.LogEntry
import ckite.rpc.NoOp
import ckite.util.Serializer
import ckite.rpc.LogEntry

class SerializerTest extends FlatSpec with Matchers with Logging {

  "a serializer" should "serialize and deserialize" in {
    val logEntry = LogEntry(1, 1, NoOp())

    val bytes = Serializer.serialize(logEntry)

    val deserialized: LogEntry = Serializer.deserialize(bytes)
  }
}