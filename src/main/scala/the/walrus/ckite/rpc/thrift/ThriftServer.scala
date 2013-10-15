package the.walrus.ckite.rpc.thrift

import com.twitter.finagle.Thrift
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import the.walrus.ckite.rpc.thrift.ThriftConverters._
import the.walrus.ckite.Cluster

class ThriftServer(cluster: Cluster) {

  def start() = {
    Thrift.serve(cluster.local.id, ckiteService)
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
    }
    new CKiteService$FinagleService(ckiteService, new TBinaryProtocol.Factory())
  }

  def stop() = {

  }

}