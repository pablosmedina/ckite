package ckite.rpc

case class AppendEntries(term: Int, leaderId: String, commitIndex: Int, prevLogIndex: Int = -1, 
						prevLogTerm: Int = -1, entries: List[LogEntry] = List())