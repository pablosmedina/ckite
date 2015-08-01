package ckite.exception

import ckite.rpc.LogEntry

/**
 * Waiting for WriteCommand commit timed out
 */
class WriteTimeoutException(logEntry: LogEntry) extends RuntimeException(s"$logEntry")