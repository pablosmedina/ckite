package ckite.http

import ckite.rpc.LogEntry
import ckite.states.StateInfo

case class Status(cluster: ClusterStatus, log: LogStatus)

case class ClusterStatus(term: Int, state: String, stateInfo: StateInfo)

case class LogStatus(length: Int, commitIndex: Int, lastEntry: Option[LogEntry])

