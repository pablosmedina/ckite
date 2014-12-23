package ckite.rpc

import ckite.Cluster
import ckite.rpc.thrift.{ ThriftServer, ThriftConnector }

class FinagleThriftRaftRpc extends RaftRpc {
  override def createServer(cluster: Cluster): RaftRpcServer = new ThriftServer(cluster)

  override def createConnector(binding: String): Connector = new ThriftConnector(binding)
}
