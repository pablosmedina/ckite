package the.walrus.ckite.rpc

import scala.util.Try
import the.walrus.ckite.Member
import the.walrus.ckite.rlog.Snapshot
import the.walrus.ckite.RemoteMember

trait Connector {

  def send(member: RemoteMember, request: RequestVote): Try[RequestVoteResponse]

  def send(member: RemoteMember, appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send(member: RemoteMember, snapshot: Snapshot)
  
  def send[T](member: RemoteMember, command: Command): T
  
  def send(member: RemoteMember, joinRequest: JoinRequest): Try[JoinResponse]
  
  def send(member: RemoteMember, getMembersRequest: GetMembersRequest): Try[GetMembersResponse]
  
  
  
}