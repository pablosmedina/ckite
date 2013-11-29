package the.walrus.ckite

import the.walrus.ckite.rpc.thrift.ThriftServer
import the.walrus.ckite.http.HttpServer

class CKite(cluster: Cluster) {

  val server = new ThriftServer(cluster)
  val httpServer = new HttpServer(cluster)
  
  def start() = {
    server.start
    httpServer.start
    cluster.start
  }   
  
  def isLeader(): Boolean = {
     false
  }
  
  def stop() = {
    server.stop
  }
  
}