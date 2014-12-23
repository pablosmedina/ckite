package ckite.rpc

import ckite.Cluster

trait RaftRpc {
  def createServer(cluster: Cluster): RaftRpcServer
  def createConnector(binding: String): Connector
}

trait RaftRpcServer {

  def start(): Unit
  def stop(): Unit

}

