package the.walrus.ckite.rpc.thrift

import com.twitter.finagle.Thrift
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import the.walrus.ckite.rpc.thrift.ThriftConverters._
import the.walrus.ckite.Cluster
import com.twitter.finagle.ListeningServer
import java.nio.ByteBuffer
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import the.walrus.ckite.rpc.Command
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import the.walrus.ckite.logcompaction.Snapshot

class ThriftServer(cluster: Cluster) {

  var finagleServer: ListeningServer = _
  
  val futurePool = FuturePool(new ThreadPoolExecutor(0, 50,
                                      15L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("ThriftWorker", true)))
  
  def start() = {
    finagleServer = Thrift.serve(cluster.local.id, ckiteService)
  }

  def ckiteService = {
    val ckiteService = new CKiteService[Future]() {
      override def sendRequestVote(requestVote: RequestVoteST) = futurePool {
//        Thread.currentThread().setName("requestVote")
        cluster on requestVote
      }
      override def sendAppendEntries(appendEntries: AppendEntriesST) = futurePool {
//        Thread.currentThread().setName("appendEntries")
        cluster on appendEntries
      }
      
      override def forwardCommand(bb: ByteBuffer) =  futurePool {
//        Thread.currentThread().setName("forwardCommand")
        val command: Command  = bb
        cluster.on[Any](command)
      }
      
      override def installSnapshot(installSnapshot: InstallSnapshotST) = futurePool {
         cluster.installSnapshot(installSnapshot)
      }
    }
    new CKiteService$FinagleService(ckiteService, new TBinaryProtocol.Factory())
  }

  def stop() = {
	finagleServer.close()
  }

}

object ThriftServer {
  def apply(cluster: Cluster) = new ThriftServer(cluster)
}