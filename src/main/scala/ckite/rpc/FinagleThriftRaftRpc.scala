package ckite.rpc

import ckite.Cluster
import ckite.rpc.thrift.{ FinagleThriftClient, FinagleThriftServer }

class FinagleThriftRaftRpc extends Rpc {
  override def createServer(cluster: Cluster): RpcServer = new FinagleThriftServer(cluster)

  override def createConnector(binding: String): RpcClient = new FinagleThriftClient(binding)
}
