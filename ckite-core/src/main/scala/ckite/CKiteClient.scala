package ckite

import java.util.concurrent.atomic.AtomicBoolean

import ckite.rpc.{ ReadCommand, RpcServer, WriteCommand }
import ckite.stats.{ Stats, ConsensusStats }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CKiteClient(raft: Raft, rpcServer: RpcServer, private[ckite] val builder: CKiteBuilder) extends CKite {

  private val stopped = new AtomicBoolean(false)

  def write[T](writeCommand: WriteCommand[T]): Future[T] = raft.onCommandReceived[T](writeCommand)

  def read[T](readCommand: ReadCommand[T]): Future[T] = raft.onCommandReceived[T](readCommand)

  def addMember(memberBinding: String) = raft.onMemberJoinReceived(memberBinding).map(_.success)

  def removeMember(memberBinding: String) = raft.onMemberLeaveReceived(memberBinding)

  def readLocal[T](readCommand: ReadCommand[T]): Future[T] = raft.onLocalReadReceived(readCommand)

  private[ckite] def isLeader: Boolean = raft.isLeader

  private[ckite] def members: Set[String] = raft.membership.members

  private[ckite] def id(): String = raft.membership.myId

  private[ckite] def stats(): Stats = raft.stats()

  def start() = {
    rpcServer.start()
    raft.start()
  }

  def stop() = {
    if (!stopped.getAndSet(true)) {
      rpcServer.stop()
      raft.stop()
    }
  }

  override val toString = s"CKite($id)"
}

object CKiteClient {
  def apply(raft: Raft, rpcServer: RpcServer, builder: CKiteBuilder) = {
    new CKiteClient(raft, rpcServer, builder)
  }
}