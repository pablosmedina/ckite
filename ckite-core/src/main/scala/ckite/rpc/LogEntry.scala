package ckite.rpc

import LogEntry._

case class LogEntry(val term: Term, val index: Index, val command: Command) {
  override def toString = s"LogEntry(term=$term,index=$index,$command)"
}

object LogEntry {
  type Index = Long
  type Term = Int
}

