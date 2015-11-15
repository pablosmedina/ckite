package ckite.mapdb

import java.nio.ByteBuffer

import ckite.SingleClusterConfiguration
import ckite.rlog.{ Vote, Snapshot }
import ckite.rpc.{ NoOp, LogEntry }
import org.scalatest.{ Matchers, FlatSpec }

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MapDBStorageTest extends FlatSpec with Matchers {

  "A MapDBStorage" should "store and retrieve snapshots" in {
    val mapdbStorage = MapDBStorage("/tmp")
    val snapshot = Snapshot(1, 1, SingleClusterConfiguration(Set("m1", "m2"), 1), ByteBuffer.wrap(Array[Byte](1)))
    mapdbStorage.saveSnapshot(snapshot)

    val someSnapshot = mapdbStorage.retrieveLatestSnapshot()

    someSnapshot shouldBe Some(snapshot)
  }

  it should "save and restore latest vote" in {
    val mapdbStorage = MapDBStorage("/tmp")

    val vote = Vote(1, "m1")

    mapdbStorage.saveVote(vote)

    mapdbStorage.retrieveLatestVote() shouldBe Some(vote)
  }

  "A MapDBStorage log" should "store and retrieve entries" in {
    val mapdbStorage = MapDBStorage("/tmp")

    mapdbStorage.log.discardEntriesFrom(1)

    val futures = (1 to 5) map { index ⇒
      mapdbStorage.log.append(LogEntry(1, index, NoOp()))
    }

    Await.ready(Future.sequence(futures), 3 seconds)

    mapdbStorage.log.size shouldEqual 5
    (1 to 5) foreach { index ⇒
      mapdbStorage.log.getEntry(index) shouldEqual LogEntry(1, index, NoOp())
    }

  }
}
