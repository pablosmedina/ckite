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
      override def sendRequestVote(requestVote: RequestVoteST) = futurePool {
        cluster on requestVote
      }
      override def sendAppendEntries(appendEntries: AppendEntriesST) = futurePool {
        cluster on appendEntries
      }
      
      override def forwardCommand(bb: ByteBuffer) =  futurePool {
        val command: Command  = bb
        cluster.on[Any](command)
      }
      
      override def installSnapshot(installSnapshot: InstallSnapshotST) = futurePool {
         cluster.installSnapshot(installSnapshot)
      }
      
      override def join(joinRequest: JoinRequestST) = futurePool {
        val success = cluster.addMember(joinRequest._1)
        JoinResponseST(true)
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