package ckite

import java.util.concurrent.TimeoutException

import ckite.exception.LeaderTimeoutException
import ckite.util.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future, Promise }

case class LeaderAnnouncer(membership: Membership, configuration: Configuration) extends Logging {

  private val waitForLeaderTimeout = configuration.waitForLeaderTimeout millis
  private val promise = Promise[Member]()

  def announce(leaderId: String) = {
    val leader: Member = if (membership.myId == leaderId) membership.localMember else membership.get(leaderId).getOrElse(unknownAnnouncedLeader(leaderId))
    promise.trySuccess(leader)
  }

  def onElection = {
    if (isLeaderAnnounced) LeaderAnnouncer(membership, configuration) else this
  }

  def onStepDown = {
    if (isLeaderAnnounced) LeaderAnnouncer(membership, configuration) else this
  }

  def onLeader[T](block: Member ⇒ Future[T]): Future[T] = {
    leader().flatMap(block(_))
  }

  def awaitLeader: Member = {
    try {
      Await.result(promise.future, waitForLeaderTimeout)
    } catch {
      case e: TimeoutException ⇒ {
        logger.warn("Wait for Leader in {} timed out after {}", waitForLeaderTimeout)
        throw new LeaderTimeoutException(e)
      }
    }
  }

  def isLeaderAnnounced = promise.isCompleted

  private def leader(): Future[Member] = {
    promise.future
  }

  private def unknownAnnouncedLeader(leaderId: String) = {
    logger.info(s"Unknown Leader $leaderId")
    throw new RuntimeException("Announced Leader member is unknown")
  }
}
