package ckite.rpc

import ckite.rpc.thrift.{FinagleThriftClient, FinagleThriftServer}
import com.typesafe.config.ConfigFactory

object FinagleThriftRpc extends Rpc {

  private val config = ConfigFactory.load()

  override def createServer(rpcService: RpcService): RpcServer = FinagleThriftServer(rpcService, config)

  override def createClient(binding: String): RpcClient = FinagleThriftClient(binding)

}
