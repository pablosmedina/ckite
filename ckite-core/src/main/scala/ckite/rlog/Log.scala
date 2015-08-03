package ckite.rlog

import ckite.rpc.LogEntry
import ckite.rpc.LogEntry.Index

import scala.concurrent.Future

trait Log {

  def append(entry: LogEntry): Future[Unit]

  def rollLog(upToIndex: Index): Unit

  def getEntry(index: Index): LogEntry //TODO: change it to Option[LogEntry]

  def getLastIndex: Long

  def discardEntriesFrom(index: Index): Unit

  def size: Long

  def close(): Unit

}

