package ckite.rpc

import ckite.rpc.thrift.{ FinagleThriftClient, FinagleThriftServer }
import com.typesafe.config.Config

object FinagleThriftRpc extends Rpc {

  override def createServer(rpcService: RpcService, config: Config): RpcServer = FinagleThriftServer(rpcService, config)

  override def createClient(address: String): RpcClient = FinagleThriftClient(address)

}