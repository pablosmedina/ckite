package ckite

import ckite.rlog.Storage
import ckite.rpc._
import ckite.statemachine.StateMachine
import ckite.util.{ ConcurrencySupport, Logging }

import scala.concurrent.Future

class Raft(stateMachine: StateMachine, rpc: Rpc, storage: Storage, configuration: Configuration) extends RpcService with ConcurrencySupport with Logging {

  val consensus = Consensus(this, storage, configuration)
  val membership = Membership(LocalMember(this, configuration), rpc, configuration)
  val log = RLog(this, stateMachine, storage, configuration)

  def start() = {
    logger.info(s"Starting CKite ${membership.myId}...")
    initializeLog()
    if (configuration.bootstrap) {
      bootstrapStart()
    } else if (!isInitialized) {
      joinStart()
    } else {
      normalStart()
    }
  }

  def initializeLog() = log.initialize()

  def joinStart() = {
    logger.info("CKite not initialized. Join start")
    consensus.startAsJoiner()
  }

  private def normalStart() = {
    logger.info("CKite already initialized. Simple start")
    consensus.startAsFollower()
  }

  private def bootstrapStart() = {
    logger.info("Bootstrapping a new CKite consensus cluster...")

    membership.bootstrap()
    log.bootstrap()

    consensus.startAsBootstrapper()

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

object Raft {
  def apply(stateMachine: StateMachine, rpc: Rpc, storage: Storage, configuration: Configuration) = {
    new Raft(stateMachine, rpc, storage, configuration)
  }
}