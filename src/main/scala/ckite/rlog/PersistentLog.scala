package ckite.rlog

import ckite.rpc.LogEntry
import ckite.rpc.WriteCommand

trait PersistentLog {

  def append(entry: LogEntry): Unit
  def rollLog(upToIndex: Long): Unit
  def commit(): Unit
  def getEntry(index: Long): LogEntry
  def getLastIndex(): Long
  def discardEntriesFrom(index: Long): Unit
  def size(): Long
  def close(): Unit

}