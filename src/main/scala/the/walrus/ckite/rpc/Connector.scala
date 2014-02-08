package the.walrus.ckite.rpc

import scala.util.Try
import the.walrus.ckite.Member
import the.walrus.ckite.rlog.Snapshot
import the.walrus.ckite.RemoteMember
import com.twitter.util.Future

trait Connector {

  def send(request: RequestVote): Try[RequestVoteResponse]

  def send(appendEntries: AppendEntries): Try[AppendEntriesResponse]
  
  def send(snapshot: Snapshot): Future[Boolean]
  
  def send[T](command: Command): T
  
  def send(joinRequest: JoinRequest): Try[JoinResponse]
  
  def send(getMembersRequest: GetMembersRequest): Try[GetMembersResponse]
  
  
  
}