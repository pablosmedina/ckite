package ckite

import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import scala.collection.JavaConverters._

class Configuration(var config: Config) {

  val MinElectionTimeout = "ckite.election.minTimeout"
  val MaxElectionTimeout = "ckite.election.maxTimeout"
  val CollectVotesTimeout = "ckite.election.votingTimeout"

  val AppendEntriesTimeout = "ckite.appendEntries.timeout"
  val HeartbeatsInterval = "ckite.appendEntries.period"

  val LocalBinding = "ckite.cluster.localBinding"
  val MemberBindings = "ckite.cluster.memberBindings"
  val DynamicBootstrap = "ckite.cluster.dynamicBootstrap"
  val WaitForLeaderTimeout = "ckite.cluster.leaderTimeout"

  val AppendEntriesWorkers = "ckite.appendEntries.workers"
  val ElectionWorkers = "ckite.election.workers"
  val ThriftWorkers = "ckite.thrift.workers"

  val LogCompactionThreshold = "ckite.log.compaction.threshold"
  val FlushSize = "ckite.log.flushSize"
  val SyncEnabled = "ckite.log.syncEnabled"  
  val DataDir = "ckite.data.dir"

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
    config = config.withValue(HeartbeatsInterval, ConfigValueFactory.fromAnyRef(heartbeatsInterval))
  }

  def heartbeatsInterval: Long = {
    config.getMilliseconds(HeartbeatsInterval)
  }

  def withLocalBinding(localBinding: String) = {
    config = config.withValue(LocalBinding, ConfigValueFactory.fromAnyRef(localBinding))
  }

  def withDataDir(dataDir: String) = {
    config = config.withValue(DataDir, ConfigValueFactory.fromAnyRef(dataDir))
  }

  def dataDir: String = {
    config.getString(DataDir)
  }

  def localBinding: String = {
    config.getString(LocalBinding)
  }

  def withMemberBindings(membersBindings: Seq[String]) = {
    config = config.withValue(MemberBindings, ConfigValueFactory.fromIterable(membersBindings.asJava))
  }
  
  def withLogCompactionThreshold(threshold: Int) = {
    config = config.withValue(LogCompactionThreshold, ConfigValueFactory.fromAnyRef(threshold))
  }
  
  def withFlushSize(flushSize: Long) = {
    config = config.withValue(FlushSize, ConfigValueFactory.fromAnyRef(flushSize))
  }
  
  def withSyncEnabled(syncEnabled: Boolean) = {
    config = config.withValue(SyncEnabled, ConfigValueFactory.fromAnyRef(syncEnabled))
  }

  def withWaitForLeaderTimeout(waitForLeaderTimeout: Int) = {
    config = config.withValue(WaitForLeaderTimeout, ConfigValueFactory.fromAnyRef(waitForLeaderTimeout))
  }

  def withCollectVotesTimeout(collectVotesTimeout: Int) = {
    config = config.withValue(CollectVotesTimeout, ConfigValueFactory.fromAnyRef(collectVotesTimeout))
  }

  def waitForLeaderTimeout: Long = {
    config.getMilliseconds(WaitForLeaderTimeout)
  }

  def memberBindings: Seq[String] = {
    config.getStringList(MemberBindings).asScala
  }

  def dynamicBootstrap: Boolean = {
    config.getBoolean(DynamicBootstrap)
  }

  def withSeeds = {
    config = config.withValue(DynamicBootstrap, ConfigValueFactory.fromAnyRef(true))
  }

  def collectVotesTimeout: Long = {
    config.getMilliseconds(CollectVotesTimeout)
  }

  def logCompactionThreshold: Long = {
    config.getLong(LogCompactionThreshold)
  }

  def appendEntriesTimeout: Long = {
    config.getMilliseconds(AppendEntriesTimeout)
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
  
  def syncEnabled: Boolean = config.getBoolean(SyncEnabled)
  
  def flushSize: Long = config.getLong(FlushSize)
}