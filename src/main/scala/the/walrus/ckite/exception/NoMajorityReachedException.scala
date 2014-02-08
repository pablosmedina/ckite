package the.walrus.ckite.exception

import the.walrus.ckite.rpc.LogEntry

class NoMajorityReachedException(logEntry: LogEntry) extends RuntimeException(s"$logEntry") {

}