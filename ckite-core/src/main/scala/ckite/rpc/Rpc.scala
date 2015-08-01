package ckite.rpc

trait Rpc {
  def createServer(service: RpcService): RpcServer

  def createClient(binding: String): RpcClient

}

trait RpcServer {

  def start(): Unit

  def stop(): Unit

}

