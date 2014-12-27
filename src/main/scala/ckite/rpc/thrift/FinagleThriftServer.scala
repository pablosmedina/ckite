package ckite.rpc.thrift

import java.nio.ByteBuffer
import java.util.concurrent.{ SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import ckite.Cluster
import ckite.rpc.RpcServer
import ckite.rpc.thrift.ThriftConverters._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.{ ListeningServer, Thrift }
import com.twitter.util.{ Future, FuturePool, Promise }
import org.apache.thrift.protocol.TBinaryProtocol

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future ⇒ ScalaFuture }
import scala.util.{ Failure, Success }

class FinagleThriftServer(cluster: Cluster) extends RpcServer {
  var closed = false
  var finagleServer: ListeningServer = _

  def start() = {
    val localPort = cluster.local.id.split(":")(1)
    finagleServer = Thrift.serve(s":$localPort", ckiteService)
  }

  implicit def toTwitterFuture[T](scalaFuture: ScalaFuture[T]): Future[T] = {
    val promise = Promise[T]
    scalaFuture.onComplete {
      case Success(value) ⇒ promise.setValue(value)
      case Failure(t)     ⇒ promise.raise(t)
    }
    promise
  }

  def ckiteService = {
    val ckiteService = new CKiteService[Future]() {

      override def sendRequestVote(requestVote: RequestVoteST): Future[RequestVoteResponseST] = {
        cluster.onRequestVoteReceived(requestVote).map[RequestVoteResponseST](r ⇒ r)
      }

      override def sendAppendEntries(appendEntries: AppendEntriesST): Future[AppendEntriesResponseST] = {
        cluster.onAppendEntriesReceived(appendEntries).map[AppendEntriesResponseST](r ⇒ r)
      }

      override def sendCommand(bb: ByteBuffer): Future[ByteBuffer] = {
        cluster.onCommandReceived[Any](bb).map[ByteBuffer](r ⇒ r)
      }

      override def sendJoinMember(joinRequest: JoinMemberST): Future[JoinMemberResponseST] = {
        cluster.onMemberJoinReceived(joinRequest._1).map[JoinMemberResponseST](r ⇒ r)
      }

      override def sendInstallSnapshot(installSnapshot: InstallSnapshotST) = {
        cluster.onInstallSnapshotReceived(installSnapshot).map[InstallSnapshotResponseST](r ⇒ r)
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

object FinagleThriftServer {
  def apply(cluster: Cluster) = new FinagleThriftServer(cluster)
}