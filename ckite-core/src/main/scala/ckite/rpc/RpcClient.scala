package ckite.rpc

import scala.concurrent.Future

trait RpcClient {

  def send(request: RequestVote): Future[RequestVoteResponse]

  def send(appendEntries: AppendEntries): Future[AppendEntriesResponse]

  def send(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse]

  def send[T](command: Command): Future[T]

  def send(joinMember: JoinMember): Future[JoinMemberResponse]

}