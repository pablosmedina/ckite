package ckite

import ckite.rlog.Storage
import ckite.rpc._
import ckite.statemachine.StateMachine
import ckite.util.{ ConcurrencySupport, Logging }

import scala.concurrent.Future

class Raft(stateMachine: StateMachine, rpc: Rpc, storage: Storage, configuration: Configuration) extends RpcService with Logging with ConcurrencySupport {

  val consensus = Consensus(this, storage, configuration)
  val membership = Membership(LocalMember(this, configuration), rpc, configuration)
  val log = RLog(this, stateMachine, storage, configuration)

  def start() = {
    logger.info("Starting CKite...")

    log.initialize()

    if (configuration.bootstrap) {
      startBootstrap
    } else if (!isInitialized) {
      consensus.startJoiner
    } else {
      startNormal
    }
  }

  private def startNormal = {
    //start as a normal follower
    logger.info("Existing configuration. Start normal")
    consensus.start()
  }

  private def startBootstrap = {
    logger.info("Bootstrapping a new CKite consensus cluster...")

    membership.bootstrap()
    consensus.bootstrap()
    log.bootstrap()

    consensus.leaderAnnouncer.awaitLeader
  }

  private def isInitialized = membership.isInitialized

  def stop() = {
    logger.info(s"Stopping CKite ${membership.myId}...")
    consensus.stop()
    log.stop()
  }

  def onRequestVoteReceived(requestVote: RequestVote): Future[RequestVoteResponse] = {
    logger.debug("RequestVote received: {}", requestVote)
    consensus.onRequestVote(requestVote)
  }

  def onAppendEntriesReceived(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    logger.trace(s"Received $appendEntries")
    consensus.onAppendEntries(appendEntries)
  }

  def onCommandReceived[T](command: Command): Future[T] = {
    logger.debug("Command received: {}", command)
    consensus.onCommand[T](command)
  }

  def onInstallSnapshotReceived(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = {
    logger.debug("InstallSnapshot received")
    consensus.onInstallSnapshot(installSnapshot)
  }

  def onLocalReadReceived[T](read: ReadCommand[T]) = {
    log.execute(read)
  }

  def onMemberJoinReceived(member: String): Future[JoinMemberResponse] = {
    logger.info(s"Join member $member request received")
    consensus.onMemberJoin(member)
  }

  def onMemberLeaveReceived(member: String): Future[Boolean] = {
    logger.info(s"Leave member $member request received")
    consensus.onMemberLeave(member)
  }

  def isLeader = {
    consensus.isLeader
  }

}