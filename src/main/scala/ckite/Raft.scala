package ckite

import java.util.concurrent.atomic.AtomicBoolean

import ckite.rpc.thrift.FinagleThriftServer
import ckite.rpc.{ ReadCommand, WriteCommand }

import scala.concurrent.Future

class Raft(private[ckite] val cluster: Cluster, private[ckite] val builder: RaftBuilder) {

  private val thrift = FinagleThriftServer(cluster)
  private val stopped = new AtomicBoolean(false)

  def write[T](writeCommand: WriteCommand[T]): Future[T] = cluster.onCommandReceived[T](writeCommand)

  def read[T](readCommand: ReadCommand[T]): Future[T] = cluster.onCommandReceived[T](readCommand)

  def readLocal[T](readCommand: ReadCommand[T]): T = cluster.onLocalReadReceived(readCommand)

  def addMember(memberBinding: String) = cluster.onMemberJoinReceived(memberBinding)

  def removeMember(memberBinding: String) = cluster.onMemberLeaveReceived(memberBinding)

  def isLeader: Boolean = cluster.isLeader

  def getMembers: List[String] = cluster.membership.getMembers

  def status = cluster.getStatus

  def start = {
    thrift start

    cluster start
  }

  def stop = {
    if (!stopped.getAndSet(true)) {
      thrift stop

      cluster stop
    }
  }
}