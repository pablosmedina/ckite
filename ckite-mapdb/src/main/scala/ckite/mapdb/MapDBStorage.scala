package ckite.mapdb

import ckite.rlog._
import ckite.util.Serializer
import org.mapdb.DBMaker

class MapDBStorage(dataDir: String) extends Storage with FileSupport {

  private val logDir = s"$dataDir/log"
  private val snapshotsDir = s"$dataDir/snapshots"
  private val stateDir = s"$dataDir/state"

  override val log: Log = new MapDBPersistentLog(logDir)

  private val stateDB = DBMaker.newFileDB(file(stateDir, "ckite-mapdb-state")).make()
  private val voteTerm = stateDB.getAtomicInteger("term")
  private val voteMember = stateDB.getAtomicString("memberId")

  private val snapshotsDB = DBMaker.newFileDB(file(snapshotsDir, "ckite-mapdb-snapshots")).mmapFileEnable().make()
  private val snapshotsMap = snapshotsDB.getHashMap[String, Array[Byte]]("snapshotsMap")

  override def retrieveLatestSnapshot(): Option[Snapshot] = {
    Option(snapshotsMap.get("snapshot")).map(deserializeSnapshot)
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

  private def serializeSnapshot(snapshot: Snapshot): Array[Byte] = Serializer.serialize(snapshot)

  private def deserializeSnapshot(bytes: Array[Byte]): Snapshot = Serializer.deserialize(bytes)

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

object MapDBStorage {
  def apply(dataDir: String) = new MapDBStorage(dataDir)
}
