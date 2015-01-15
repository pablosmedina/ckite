package ckite.rpc

import scala.concurrent.Future

trait RpcService {

  def onRequestVoteReceived(requestVote: RequestVote): Future[RequestVoteResponse]
  def onAppendEntriesReceived(appendEntries: AppendEntries): Future[AppendEntriesResponse]
  def onCommandReceived[T](command: Command): Future[T]
  def onInstallSnapshotReceived(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse]
  def onMemberJoinReceived(memberBinding: String): Future[JoinMemberResponse]

}
