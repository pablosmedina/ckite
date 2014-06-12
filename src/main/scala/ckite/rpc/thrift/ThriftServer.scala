package ckite.rpc.thrift

import java.nio.ByteBuffer
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future => ScalaFuture }
import scala.util.Failure
import scala.util.Success

import org.apache.thrift.protocol.TBinaryProtocol

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Thrift
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Promise

import ckite.Cluster
import ckite.rpc.Command
import ckite.rpc.thrift.ThriftConverters._

class ThriftServer(cluster: Cluster) {
  var closed = false
  var finagleServer: ListeningServer = _

  def start() = {
    val localPort = cluster.local.id.split(":")(1)
    finagleServer = Thrift.serve(s":$localPort", ckiteService)
  }

  implicit def toTwitterFuture[T](scalaFuture: ScalaFuture[T]): Future[T] = {
    val promise = Promise[T]
    scalaFuture.onComplete {
      case Success(value) => promise.setValue(value)
      case Failure(t) => promise.raise(t)
    }
    promise
  }

  def ckiteService = {
    val ckiteService = new CKiteService[Future]() {

      override def sendRequestVote(requestVote: RequestVoteST): Future[RequestVoteResponseST] = {
        (cluster on requestVote).map[RequestVoteResponseST] { response => response }
      }
      override def sendAppendEntries(appendEntries: AppendEntriesST): Future[AppendEntriesResponseST] = {
        (cluster on appendEntries).map[AppendEntriesResponseST] { response => response }
      }

      override def forwardCommand(bb: ByteBuffer): Future[ByteBuffer] = {
        val command: Command = bb
        (cluster.on[Any](command)).map[ByteBuffer] { response => response }
      }

      override def join(joinRequest: JoinRequestST): Future[JoinResponseST] = {
        (cluster.addMember(joinRequest._1)).map { response => JoinResponseST(response) }
      }

      override def getMembers() = {
        Future.value(GetMembersResponseST(true, cluster.getMembers()))
      }
      override def installSnapshot(installSnapshot: InstallSnapshotST) = futurePool {
        cluster.installSnapshot(installSnapshot)
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

  val futurePool = FuturePool(new ThreadPoolExecutor(0, cluster.configuration.thriftWorkers,
    15L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    new NamedPoolThreadFactory("Thrift-worker", true)))

}

object ThriftServer {
  def apply(cluster: Cluster) = new ThriftServer(cluster)
}