package ckite.rlog

import ckite.rpc.LogEntry
import ckite.rpc.WriteCommand

trait PersistentLog {

  def append(entry: LogEntry): Unit
  def rollLog(upToIndex: Long)
  def commit
  def getEntry(index: Long): LogEntry
  def getLastIndex: Long
  def discardEntriesFrom(index: Long)
//  def discardEntriesUntil(index: Long)
//  def remove(index: Long)
  def size: Long
  def close
  
}