package the.walrus.ckite.rpc.thrift

import scala.util.Try
import the.walrus.ckite.Member
import the.walrus.ckite.rpc._
import the.walrus.ckite.util.Logging
import com.twitter.finagle.Thrift
import com.twitter.util.Future
import scala.util.Success
import java.nio.ByteBuffer
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import scala.util.Failure
import the.walrus.ckite.rpc.thrift.ThriftConverters._
import scala.collection.concurrent.TrieMap
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.conversions.time._
import com.twitter.finagle.service.RetryPolicy
import com.twitter.util.Throw


class ThriftConnector(binding: String) extends Connector with Logging {

  val client = new CKiteService.FinagledClient(ClientBuilder().hosts(binding)
  				.tcpConnectTimeout(20.milliseconds).connectTimeout(20.milliseconds)
  				.retryPolicy(NoRetry).codec(ThriftClientFramedCodec()).failFast(false)
  				.hostConnectionLimit(1).build())
  
  override def send(member: Member, request: RequestVote): Try[RequestVoteResponse] = {
    Try {
      client.sendRequestVote(request).get
    }
  }
  
  override def send(member: Member, appendEntries: AppendEntries): Try[AppendEntriesResponse] = {
   Try {
      client.sendAppendEntries(appendEntries).get
    }
  }
  
  override def send(leader: Member, command: Command) = {
  }
  

}

object NoRetry extends RetryPolicy[com.twitter.util.Try[Nothing]] {
       def apply(e: com.twitter.util.Try[Nothing]) = {
          None
      }
}
