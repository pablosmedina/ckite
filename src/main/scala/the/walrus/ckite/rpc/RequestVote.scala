package the.walrus.ckite.rpc

case class RequestVote(memberId: String, term: Int, lastLogIndex: Int = -1, lastLogTerm: Int = -1)