package ckite.rpc.thrift

import com.twitter.finagle.Thrift
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import ckite.rpc.thrift.ThriftConverters._
import ckite.Cluster
import com.twitter.finagle.ListeningServer
import java.nio.ByteBuffer
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import ckite.rpc.Command
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.rlog.Snapshot
import com.twitter.util.Promise
import scala.concurrent.{ Future => ScalaFuture }
import ckite.rpc.AppendEntries
import scala.concurrent.Await
import scala.concurrent.duration._
import ckite.rpc.RequestVoteResponse
import ckite.rpc.AppendEntriesResponse

class ThriftServer(cluster: Cluster) {
  var closed = false
  var finagleServer: ListeningServer = _

  val futurePool = FuturePool(new ThreadPoolExecutor(0, cluster.configuration.thriftWorkers,
    15L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    new NamedPoolThreadFactory("Thrift-worker", true)))

  def start() = {
    val localPort = cluster.local.id.split(":")(1)
    finagleServer = Thrift.serve(s":$localPort", ckiteService)
  }

  def ckiteService = {
    val ckiteService = new CKiteService[Future]() {

      override def sendRequestVote(requestVote: RequestVoteST):Future[RequestVoteResponseST] =  futurePool{
        Await.result[RequestVoteResponse](cluster on requestVote, 3 seconds)
      }
      override def sendAppendEntries(appendEntries: AppendEntriesST):Future[AppendEntriesResponseST] =futurePool  {
        Await.result[AppendEntriesResponse](cluster on appendEntries, 3 seconds)
      }

      override def forwardCommand(bb: ByteBuffer) = futurePool {
        val command: Command = bb
        Await.result[Any](cluster.on[Any](command), 3 seconds)
      }

      override def installSnapshot(installSnapshot: InstallSnapshotST) = futurePool {
        cluster.installSnapshot(installSnapshot)
      }

      override def join(joinRequest: JoinRequestST) = futurePool {
        val success = cluster.addMember(joinRequest._1)
        JoinResponseST(Await.result[Boolean](success, 3 seconds))
      }

      override def getMembers() = futurePool {
        GetMembersResponseST(true, cluster.getMembers())
      }
    }
    new CKiteService$FinagleService(ckiteService, new TBinaryProtocol.Factory())
  }

  def stop() = synchronized {
    if (!closed) {
      futurePool.executor.shutdownNow()
      finagleServer.close()
      closed = true
    }
  }

}

object ThriftServer {
  def apply(cluster: Cluster) = new ThriftServer(cluster)
}