package ckite.mapdb

import java.io.{DataOutput, DataInput, File}
import java.nio.ByteBuffer

import ckite.rlog._
import org.mapdb.{Serializer, DBMaker}

class MapDBStorage(dataDir: String) extends Storage with FileSupport {

  private val logDir = s"$dataDir/log"
  private val snapshotsDir = s"$dataDir/snapshots"
  private val stateDir = s"$dataDir/state"

  override val log: Log = new MapDBPersistentLog(logDir)

  val stateDB = DBMaker.newFileDB(file(stateDir,"ckite-mapdb-state")).make()
  private val voteTerm = stateDB.createAtomicInteger("term",0)
  private val voteMember = stateDB.createAtomicString("memberId","")

  private val snapshotsDB = DBMaker.newFileDB(file(snapshotsDir,"ckite-mapdb-snapshots")).mmapFileEnable().make()
  private val snapshotsMap = snapshotsDB.getHashMap[String, ByteBuffer]("snapshotsMap")

  override def retrieveLatestSnapshot(): Option[Snapshot] = {
    Some(deserializeSnapshot(snapshotsMap.get("snapshot")))
  }

  override def saveVote(vote: Vote): Unit = {
    voteTerm.set(vote.term)
    voteMember.set(vote.member)
    stateDB.commit()
  }

  override def saveSnapshot(snapshot: Snapshot): Unit = {
    snapshotsMap.put("snapshot", serializeSnapshot(snapshot))
    snapshotsDB.commit()
  }

  private def serializeSnapshot(snapshot: Snapshot): ByteBuffer = ???

  private def deserializeSnapshot(buffer: ByteBuffer): Snapshot = ???

  override def retrieveLatestVote(): Option[Vote] = {
    val term = voteTerm.get()
    val member = voteMember.get()
    if (term == 0 && member.isEmpty) {
      None
    } else {
      Some(Vote(term, member))
    }
  }

}
