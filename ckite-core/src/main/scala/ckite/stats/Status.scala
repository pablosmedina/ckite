package ckite.stats

import ckite.rpc.LogEntry

case class Status(cluster: ClusterStatus, log: LogStatus)

case class ClusterStatus(term: Int, state: String, stateInfo: StateInfo)

case class LogStatus(length: Long, commitIndex: Long, lastEntry: Option[LogEntry])

