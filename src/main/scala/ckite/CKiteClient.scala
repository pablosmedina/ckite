package ckite

import java.util.concurrent.atomic.AtomicBoolean

import ckite.rpc.{ ReadCommand, RpcServer, WriteCommand }

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class CKiteClient(cluster: Cluster, rpcServer: RpcServer, private[ckite] val builder: CKiteBuilder) extends CKite {

  private val stopped = new AtomicBoolean(false)

  def write[T](writeCommand: WriteCommand[T]): Future[T] = cluster.onCommandReceived[T](writeCommand)

  def read[T](readCommand: ReadCommand[T]): Future[T] = cluster.onCommandReceived[T](readCommand)

  def addMember(memberBinding: String) = cluster.onMemberJoinReceived(memberBinding).map(_.success)

  def removeMember(memberBinding: String) = cluster.onMemberLeaveReceived(memberBinding)

  private[ckite] def readLocal[T](readCommand: ReadCommand[T]): T = cluster.onLocalReadReceived(readCommand)

  private[ckite] def isLeader(): Boolean = cluster.isLeader

  private[ckite] def getMembers(): List[String] = cluster.membership.getMembers()

  def start = {
    rpcServer start

    cluster start
  }

  def stop = {
    if (!stopped.getAndSet(true)) {
      rpcServer stop

      cluster stop
    }
  }
}