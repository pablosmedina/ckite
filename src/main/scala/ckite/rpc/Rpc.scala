package ckite.rpc

import ckite.Cluster

trait Rpc {
  def createServer(cluster: Cluster): RpcServer
  def createConnector(binding: String): RpcClient
}

trait RpcServer {

  def start(): Unit
  def stop(): Unit

}

