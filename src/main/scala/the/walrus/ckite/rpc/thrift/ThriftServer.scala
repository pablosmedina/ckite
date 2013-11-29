package the.walrus.ckite.rpc.thrift

import com.twitter.finagle.Thrift
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import the.walrus.ckite.rpc.thrift.ThriftConverters._
import the.walrus.ckite.Cluster
import com.twitter.finagle.ListeningServer
import java.nio.ByteBuffer

class ThriftServer(cluster: Cluster) {

  var finagleServer: ListeningServer = _
  
  def start() = {
    finagleServer = Thrift.serve(cluster.local.id, ckiteService)
  }

  def ckiteService = {
    val ckiteService = new CKiteService[Future]() {
      override def sendRequestVote(requestVote: RequestVoteST) = Future[RequestVoteResponseST] {
        Thread.currentThread().setName("requestVote")
        cluster.onMemberRequestingVote(requestVote)
      }
      override def sendAppendEntries(request: AppendEntriesST) = Future[AppendEntriesResponseST] {
        Thread.currentThread().setName("appendEntries")
        cluster.onAppendEntriesReceived(request)
      }
      
      override def forwardCommand(commandByteBuffer: ByteBuffer) =  Future[Unit] {
        Thread.currentThread().setName("forwardCommand")
        cluster.onCommandReceived(commandByteBuffer)
      }
    }
    new CKiteService$FinagleService(ckiteService, new TBinaryProtocol.Factory())
  }

  def stop() = {
	finagleServer.close()
  }

}