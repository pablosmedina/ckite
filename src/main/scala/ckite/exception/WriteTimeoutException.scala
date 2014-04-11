package ckite.exception

import ckite.rpc.LogEntry

class WriteTimeoutException(logEntry: LogEntry) extends RuntimeException(s"$logEntry")