package ckite

import com.typesafe.config.Config

trait ConfigSupport {

  implicit val config: Config

  val Id = "ckite.finagle.listen-address"
  val Bootstrap = "ckite.bootstrap"

  val MinElectionTimeout = "ckite.election.min-timeout"
  val MaxElectionTimeout = "ckite.election.max-timeout"
  val VotingTimeout = "ckite.election.voting-timeout"
  val ElectionWorkers = "ckite.election.workers"

  val WriteTimeout = "ckite.write-timeout"

  val HeartbeatsPeriod = "ckite.append-entries.period"
  val AppendEntriesWorkers = "ckite.append-entries.workers"

  val Members = "ckite.members"
  val LeaderTimeout = "ckite.leader-timeout"

  val ListenAddress = "ckite.finagle.listen-address"
  val ThriftWorkers = "ckite.finagle.thrift.workers"

  val CompactionThreshold = "ckite.log.compaction-threshold"
  val FlushSize = "ckite.log.flush-size"
  val Sync = "ckite.log.sync"
  val DataDir = "ckite.datadir"

}