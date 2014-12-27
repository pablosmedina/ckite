package ckite.rpc

case class AppendEntries(term: Int, leaderId: String, commitIndex: Long, prevLogIndex: Long = -1,
    prevLogTerm: Int = -1, entries: List[LogEntry] = List()) {
  override def toString = s"AppendEntries(term=$term,leaderId=$leaderId,commitIndex=$commitIndex,prevLogIndex=$prevLogIndex,prevLogTerm=$prevLogTerm,entries=$entries)"
}