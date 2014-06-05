package ckite.rpc.thrift

import scala.util.Try
import ckite.Member
import ckite.rpc._
import ckite.util.Logging
import com.twitter.finagle.Thrift
import com.twitter.util.Future
import scala.concurrent.{ Future => ScalaFuture }
import scala.util.Success
import java.nio.ByteBuffer
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import scala.util.Failure
import ckite.rpc.thrift.ThriftConverters._
import scala.collection.concurrent.TrieMap
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.conversions.time._
import com.twitter.finagle.service.RetryPolicy
import com.twitter.util.Throw
import ckite.rlog.Snapshot
import ckite.RemoteMember
import scala.concurrent.Promise

class ThriftConnector(binding: String) extends Connector with Logging {

  val client = new CKiteService.FinagledClient(ClientBuilder().hosts(binding)
    .retryPolicy(NoRetry).codec(ThriftClientFramedCodec()).failFast(false)
    .hostConnectionLimit(10).hostConnectionCoresize(1).requestTimeout(Duration(60, TimeUnit.SECONDS)).build())

  override def send(request: RequestVote): ScalaFuture[RequestVoteResponse] = {
    LOG.debug(s"Sending $request to $binding")
    val f = client.sendRequestVote(request)
    val promise = Promise[RequestVoteResponse]()
    f.onSuccess(value => promise.success(value))
    f.onFailure(e => promise.failure(e))
    promise.future
  }

  override def send(appendEntries: AppendEntries): ScalaFuture[AppendEntriesResponse] = {
    LOG.trace(s"Sending $appendEntries to $binding")
    val f = client.sendAppendEntries(appendEntries)
    val promise = Promise[AppendEntriesResponse]()
    f.onSuccess(value => promise.success(value))
    f.onFailure(e => promise.failure(e))
    promise.future
  }

  override def send[T](command: Command): ScalaFuture[T] = {
    val future = client.forwardCommand(command)
    val promise = Promise[T]()
    future.onSuccess(value => promise.success(value))
    future.onFailure(e => promise.failure(e))
    promise.future
  }

  override def send(snapshot: Snapshot) = {
    val future: Future[Boolean] = client.installSnapshot(snapshot)
    future
  }

  override def send(joinRequest: JoinRequest): ScalaFuture[JoinResponse] = {
    val future = client.join(joinRequest)
    val promise = Promise[JoinResponse]()
    future.onSuccess(value => promise.success(value))
    future.onFailure(e => promise.failure(e))
    promise.future
  }

  override def send(getMembersRequest: GetMembersRequest): ScalaFuture[GetMembersResponse] = {
    val future = client.getMembers()
    val promise = Promise[GetMembersResponse]()
    future.onSuccess(value => promise.success(value))
    future.onFailure(e => promise.failure(e))
    promise.future
  }

  private implicit def toScalaFuture[T](twitterFuture: Future[T]): ScalaFuture[T] = {
    val promise = Promise[T]()
    twitterFuture.onSuccess(value => promise.success(value))
    twitterFuture.onFailure(e => promise.failure(e))
    promise.future
  }

}

object NoRetry extends RetryPolicy[com.twitter.util.Try[Nothing]] {
  def apply(e: com.twitter.util.Try[Nothing]) = {
    None
  }
}
