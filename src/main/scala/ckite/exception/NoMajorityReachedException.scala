package ckite.exception

import ckite.rpc.LogEntry

class NoMajorityReachedException(logEntry: LogEntry) extends RuntimeException(s"$logEntry") {

}