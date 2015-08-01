package ckite.exception

import java.util.concurrent.TimeoutException

/**
 * Raised when waiting for a Leader to be elected timed out
 */
class LeaderTimeoutException(exception: TimeoutException) extends RuntimeException(exception)