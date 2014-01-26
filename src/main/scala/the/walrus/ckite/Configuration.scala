package the.walrus.ckite

import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import scala.collection.JavaConverters._

class Configuration(var config: Config) {

  val MinElectionTimeout = "ckite.election.minTimeout"
  val MaxElectionTimeout = "ckite.election.maxTimeout"
  val CollectVotesTimeout = "ckite.election.votingTimeout"
    
  val ReplicationTimeout = "ckite.replication.timeout"
  val HeartbeatsInterval = "ckite.heartbeats.period"
    
  val MembersBindings = "ckite.cluster.membersBindings"
  val WaitForLeaderTimeout = "ckite.cluster.waitForLeaderTimeout"
    
  val FixedLogSizeCompaction = "ckite.log.compaction.fixedLogSize"  
  val LocalBinding = "localBinding"
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

  def withMembersBindings(membersBindings: Seq[String]) = {
    config = config.withValue(MembersBindings, ConfigValueFactory.fromIterable(membersBindings.asJava))
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

  def membersBindings: Seq[String] = {
    config.getStringList(MembersBindings).asScala
  }
  
  def collectVotesTimeout: Long = { 
    config.getMilliseconds(CollectVotesTimeout)
  }
  
  def fixedLogSizeCompaction: Long = {
    config.getLong(FixedLogSizeCompaction)
  }

}