package ckite.exception

/**
 * Raised when waiting for a Leader loses its leadership.
 * This can happen during reads on a Leader that gets partitioned from the rest of the cluster
 */
case class LostLeadershipException(reason: String) extends RuntimeException(reason)