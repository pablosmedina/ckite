package the.walrus.ckite.http

import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.states.StateInfo

case class Status(cluster: ClusterStatus, log: LogStatus)

case class ClusterStatus(term: Int, state: String, stateInfo: StateInfo)

case class LogStatus(length: Int, commitIndex: Int, lastLog: Option[LogEntry])

