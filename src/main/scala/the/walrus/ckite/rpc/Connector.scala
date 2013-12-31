package the.walrus.ckite.rpc

import scala.util.Try
import the.walrus.ckite.Member

trait Connector {

  def send(member: Member, request: RequestVote): Try[RequestVoteResponse]

  def send(member: Member, appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send[T](member: Member, command: Command): T
  
}