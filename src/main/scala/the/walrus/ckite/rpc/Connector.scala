package the.walrus.ckite.rpc

import scala.util.Try
import the.walrus.ckite.Member

trait Connector {

  def send(member: Member, request: RequestVote): Try[RequestVoteResponse]

  def sendHeartbeat(member: Member, appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send(member: Member, appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send(member: Member, command: Command)
  
}