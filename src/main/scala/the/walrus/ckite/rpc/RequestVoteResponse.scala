package the.walrus.ckite.rpc

case class RequestVoteResponse(currentTerm: Int, granted: Boolean)