package ckite.rpc

import scala.util.Try
import ckite.Member
import ckite.rlog.Snapshot
import ckite.RemoteMember
import scala.concurrent.Future

trait Connector {

  def send(request: RequestVote): Future[RequestVoteResponse]

  def send(appendEntries: AppendEntries): Future[AppendEntriesResponse]
  
  def send(snapshot: Snapshot): Future[Boolean]
  
  def send[T](command: Command): Future[T]
  
  def send(joinRequest: JoinRequest): Future[JoinResponse]
  
  def send(getMembersRequest: GetMembersRequest): Future[GetMembersResponse]
  
}