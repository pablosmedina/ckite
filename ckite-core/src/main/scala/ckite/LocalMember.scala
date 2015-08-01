package ckite

import ckite.rpc.Command

import scala.concurrent.Future

case class LocalMember(raft: Raft, configuration: Configuration) extends Member(configuration.localBinding) {

  private def consensus = raft.consensus

  override def forwardCommand[T](command: Command): Future[T] = {
    consensus.onCommand(command)
  }

}