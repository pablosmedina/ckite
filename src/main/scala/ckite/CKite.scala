package ckite

import ckite.rpc.{ ReadCommand, WriteCommand }

import scala.concurrent.Future

trait CKite {

  def start: Unit
  def stop: Unit

  def write[T](writeCommand: WriteCommand[T]): Future[T]
  def read[T](readCommand: ReadCommand[T]): Future[T]

  def addMember(memberBinding: String): Future[Boolean]
  def removeMember(memberBinding: String): Future[Boolean]

}
