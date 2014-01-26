package the.walrus.ckite.rpc

import scala.util.Try
import the.walrus.ckite.Member
import the.walrus.ckite.rlog.Snapshot

trait Connector {

  def send(member: Member, request: RequestVote): Try[RequestVoteResponse]

  def send(member: Member, appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send(member: Member, snapshot: Snapshot)
  
  def send[T](member: Member, command: Command): T
  
}