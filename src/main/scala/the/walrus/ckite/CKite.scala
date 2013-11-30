package the.walrus.ckite

import the.walrus.ckite.rpc.thrift.ThriftServer
import the.walrus.ckite.http.HttpServer

class CKite(cluster: Cluster) {

  val thrift = ThriftServer(cluster)
  val http = HttpServer(cluster)
  
  def start = {
    thrift start
    
    http start
    
    cluster start
  }   
  
  def isLeader: Boolean = false
  
  def stop = thrift stop
  
}