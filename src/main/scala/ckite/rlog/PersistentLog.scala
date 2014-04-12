package ckite.rlog

import ckite.rpc.LogEntry

trait PersistentLog {

  def append(entry: LogEntry)
  def rollLog(upToIndex: Int)
  def getEntry(index: Int): LogEntry
  def getLastIndex: Int
  def remove(index: Int)
  def size: Int
  
}