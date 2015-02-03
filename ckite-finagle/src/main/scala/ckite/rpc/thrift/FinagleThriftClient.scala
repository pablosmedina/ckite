package ckite.rpc.thrift

import java.util.concurrent.TimeUnit

import ckite.rpc._
import ckite.rpc.thrift.ThriftConverters._
import ckite.util.Logging
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.service.RetryPolicy
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.util.{ Duration, Future }

import scala.concurrent.{ Promise, Future ⇒ ScalaFuture }

case class FinagleThriftClient(binding: String) extends RpcClient with Logging {

  val client = new CKiteService.FinagledClient(ClientBuilder().hosts(binding)
    .retryPolicy(NoRetry).codec(ThriftClientFramedCodec()).failFast(false)
    .hostConnectionLimit(10).hostConnectionCoresize(1).requestTimeout(Duration(60, TimeUnit.SECONDS)).build())

  override def send(request: RequestVote): ScalaFuture[RequestVoteResponse] = {
    logger.debug(s"Sending $request to $binding")
    val f = client.sendRequestVote(request)
    val promise = Promise[RequestVoteResponse]()
    f.onSuccess(value ⇒ promise.success(value))
    f.onFailure(e ⇒ promise.failure(e))
    promise.future
  }

  override def send(appendEntries: AppendEntries): ScalaFuture[AppendEntriesResponse] = {
    logger.trace(s"Sending $appendEntries to $binding")
    val f = client.sendAppendEntries(appendEntries)
    val promise = Promise[AppendEntriesResponse]()
    f.onSuccess(value ⇒ promise.success(value))
    f.onFailure(e ⇒ promise.failure(e))
    promise.future
  }

  override def send[T](command: Command): ScalaFuture[T] = {
    val future = client.sendCommand(command)
    val promise = Promise[T]()
    future.onSuccess(value ⇒ promise.success(value))
    future.onFailure(e ⇒ promise.failure(e))
    promise.future
  }

  override def send(installSnapshot: InstallSnapshot): ScalaFuture[InstallSnapshotResponse] = {
    val future = client.sendInstallSnapshot(installSnapshot)
    val promise = Promise[InstallSnapshotResponse]()
    future.onSuccess(value ⇒ promise.success(value))
    future.onFailure(e ⇒ promise.failure(e))
    promise.future
  }

  override def send(joinRequest: JoinMember): ScalaFuture[JoinMemberResponse] = {
    val future = client.sendJoinMember(joinRequest)
    val promise = Promise[JoinMemberResponse]()
    future.onSuccess(value ⇒ promise.success(value))
    future.onFailure(e ⇒ promise.failure(e))
    promise.future
  }

  private implicit def toScalaFuture[T](twitterFuture: Future[T]): ScalaFuture[T] = {
    val promise = Promise[T]()
    twitterFuture.onSuccess(value ⇒ promise.success(value))
    twitterFuture.onFailure(e ⇒ promise.failure(e))
    promise.future
  }

}

object NoRetry extends RetryPolicy[com.twitter.util.Try[Nothing]] {
  def apply(e: com.twitter.util.Try[Nothing]) = {
    None
  }
}
