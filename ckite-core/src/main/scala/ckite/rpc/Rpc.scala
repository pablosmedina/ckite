package ckite.rpc

import com.typesafe.config.Config

trait Rpc {
  def createServer(service: RpcService, config: Config): RpcServer

  def createClient(address: String): RpcClient

}

trait RpcServer {

  def start(): Unit

  def stop(): Unit

}

