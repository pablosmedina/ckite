package ckite

import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import scala.collection.JavaConverters._

class Configuration(var config: Config) {

  val Bootstrap = "ckite.bootstrap"

  val MinElectionTimeout = "ckite.election.min-timeout"
  val MaxElectionTimeout = "ckite.election.max-timeout"
  val VotingTimeout = "ckite.election.voting-timeout"
  val ElectionWorkers = "ckite.election.workers"

  val WriteTimeout = "ckite.write-timeout"

  val HeartbeatsPeriod = "ckite.append-entries.period"
  val AppendEntriesWorkers = "ckite.append-entries.workers"

  val ListenAddress = "ckite.listen-address"
  val Members = "ckite.members"
  val LeaderTimeout = "ckite.leader-timeout"

  val ThriftWorkers = "ckite.thrift.workers"

  val CompactionThreshold = "ckite.log.compaction-threshold"
  val FlushSize = "ckite.log.flush-size"
  val Sync = "ckite.log.sync"
  val DataDir = "ckite.datadir"

  def withMinElectionTimeout(minElectionTimeout: Int) = {
    config = config.withValue(MinElectionTimeout, ConfigValueFactory.fromAnyRef(minElectionTimeout))
  }

  def minElectionTimeout: Long = {
    config.getMilliseconds(MinElectionTimeout)
  }

  def withMaxElectionTimeout(maxElectionTimeout: Int) = {
    config = config.withValue(MaxElectionTimeout, ConfigValueFactory.fromAnyRef(maxElectionTimeout))
  }

  def maxElectionTimeout: Long = {
    config.getMilliseconds(MaxElectionTimeout)
  }

  def withHeartbeatsInterval(heartbeatsInterval: Int) = {
    config = config.withValue(HeartbeatsPeriod, ConfigValueFactory.fromAnyRef(heartbeatsInterval))
  }

  def heartbeatsInterval: Long = {
    config.getMilliseconds(HeartbeatsPeriod)
  }

  def withLocalBinding(localBinding: String) = {
    config = config.withValue(ListenAddress, ConfigValueFactory.fromAnyRef(localBinding))
  }

  def withDataDir(dataDir: String) = {
    config = config.withValue(DataDir, ConfigValueFactory.fromAnyRef(dataDir))
  }

  def dataDir: String = {
    config.getString(DataDir)
  }

  def localBinding: String = {
    config.getString(ListenAddress)
  }

  def withMemberBindings(membersBindings: Seq[String]) = {
    config = config.withValue(Members, ConfigValueFactory.fromIterable(membersBindings.asJava))
  }

  def withLogCompactionThreshold(threshold: Int) = {
    config = config.withValue(CompactionThreshold, ConfigValueFactory.fromAnyRef(threshold))
  }

  def withFlushSize(flushSize: Long) = {
    config = config.withValue(FlushSize, ConfigValueFactory.fromAnyRef(flushSize))
  }

  def withSyncEnabled(syncEnabled: Boolean) = {
    config = config.withValue(Sync, ConfigValueFactory.fromAnyRef(syncEnabled))
  }

  def withWaitForLeaderTimeout(waitForLeaderTimeout: Int) = {
    config = config.withValue(LeaderTimeout, ConfigValueFactory.fromAnyRef(waitForLeaderTimeout))
  }

  def withCollectVotesTimeout(collectVotesTimeout: Int) = {
    config = config.withValue(VotingTimeout, ConfigValueFactory.fromAnyRef(collectVotesTimeout))
  }

  def waitForLeaderTimeout: Long = {
    config.getMilliseconds(LeaderTimeout)
  }

  def memberBindings: Seq[String] = {
    config.getStringList(Members).asScala
  }

  def bootstrap: Boolean = {
    config.getBoolean(Bootstrap)
  }

  def bootstrap(enabled: Boolean) = {
    config = config.withValue(Bootstrap, ConfigValueFactory.fromAnyRef(enabled))
  }

  def collectVotesTimeout: Long = {
    config.getMilliseconds(VotingTimeout)
  }

  def logCompactionThreshold: Long = {
    config.getLong(CompactionThreshold)
  }

  def appendEntriesTimeout: Long = {
    config.getMilliseconds(WriteTimeout)
  }

  def appendEntriesWorkers: Int = {
    config.getInt(AppendEntriesWorkers)
  }

  def electionWorkers: Int = {
    config.getInt(ElectionWorkers)
  }

  def thriftWorkers: Int = {
    config.getInt(ThriftWorkers)
  }

  def syncEnabled: Boolean = config.getBoolean(Sync)

  def flushSize: Long = config.getLong(FlushSize)
}