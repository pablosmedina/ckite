package the.walrus.ckite

import the.walrus.ckite.rpc.thrift.ThriftServer
import the.walrus.ckite.http.HttpServer
import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.rpc.ReadCommand

class CKite(cluster: Cluster) {

  val thrift = ThriftServer(cluster)
  val http = HttpServer(cluster)
  
  def start() = {
    
    http start
    
    cluster start
    
    thrift start
  }
  
  def stop() =  {
    thrift stop
    
    http stop
    
    cluster stop
  }
  
  def write[T](writeCommand: WriteCommand): T = {
    cluster.on[T](writeCommand)
  }
  
  def read[T](readCommand: ReadCommand): T = {
    cluster.on[T](readCommand)
  }
  
  def readLocal[T](readCommand: ReadCommand): T = {
    cluster.onLocal(readCommand).asInstanceOf[T]
  }
  
  def isLeader: Boolean = cluster.awaitLeader == cluster.local
  
  
}