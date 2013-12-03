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

class ThriftServer(cluster: Cluster) {

  var finagleServer: ListeningServer = _
  
  val futurePool = FuturePool(Executors.newFixedThreadPool(8))
  
  def start() = {
    finagleServer = Thrift.serve(cluster.local.id, ckiteService)
  }

  def ckiteService = {
    val ckiteService = new CKiteService[Future]() {
      override def sendRequestVote(requestVote: RequestVoteST) = futurePool {
        Thread.currentThread().setName("requestVote")
        cluster on requestVote
      }
      override def sendAppendEntries(appendEntries: AppendEntriesST) = futurePool {
        Thread.currentThread().setName("appendEntries")
        cluster on appendEntries
      }
      
      override def forwardCommand(command: ByteBuffer) =  futurePool {
        Thread.currentThread().setName("forwardCommand")
        cluster on command
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