package the.walrus.ckite

import the.walrus.ckite.rpc.thrift.ThriftServer

class CKite(cluster: Cluster) {

  val server = new ThriftServer(cluster)
  
  def start() = {
    server.start()
    cluster.start()
  }   
  
  def isLeader(): Boolean = {
     false
  }
  
  def stop() = {
    
  }
  
}