package ckite.stats

import ckite.rpc.LogEntry

case class Stats(consensus: ConsensusStats, log: LogStats)

case class ConsensusStats(term: Int, state: String, stateInfo: StateInfo)

case class LogStats(length: Long, commitIndex: Long, lastEntry: Option[LogEntry])