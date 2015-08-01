package ckite.rpc

case class RequestVoteResponse(currentTerm: Int, granted: Boolean)