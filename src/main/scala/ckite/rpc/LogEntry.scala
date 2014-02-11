package ckite.rpc

case class LogEntry(term: Int, index: Int, command: Command) {
  override def toString = s"LogEntry(term=$term,index=$index,$command)"
}
