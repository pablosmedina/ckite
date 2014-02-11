package ckite.rpc

import scala.util.Try
import ckite.Member
import ckite.rlog.Snapshot
import ckite.RemoteMember
import com.twitter.util.Future

trait Connector {

  def send(request: RequestVote): Try[RequestVoteResponse]

  def send(appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send(snapshot: Snapshot): Future[Boolean]
  
  def send[T](command: Command): T
  
  def send(joinRequest: JoinRequest): Try[JoinResponse]
  
  def send(getMembersRequest: GetMembersRequest): Try[GetMembersResponse]
  
  
  
}