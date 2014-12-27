package ckite

import ckite.rpc.Command
import ckite.util.Logging

import scala.concurrent.Future

abstract class Member(binding: String) extends Logging {

  def id() = s"$binding"

  def forwardCommand[T](command: Command): Future[T]

  override def toString() = id

}