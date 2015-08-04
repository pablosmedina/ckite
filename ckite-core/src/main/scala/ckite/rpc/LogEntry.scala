package ckite.rpc

import LogEntry._

case class LogEntry(term: Term, index: Index, command: Command) {
  override def toString = s"LogEntry(term=$term,index=$index,$command)"
}

object LogEntry {
  type Index = Long
  type Term = Int
}