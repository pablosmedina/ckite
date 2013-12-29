package the.walrus.ckite

import the.walrus.ckite.rpc.thrift.ThriftServer
import the.walrus.ckite.http.HttpServer
import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.rpc.ReadCommand

class CKite(cluster: Cluster) {

  val thrift = ThriftServer(cluster)
  val http = HttpServer(cluster)
  
  def start() = {
    thrift start
    
    http start
    
    cluster start
  }
  
  def stop() =  {
    thrift stop
    
    http stop
    
    cluster stop
  }
  
  def write(writeCommand: WriteCommand): Any = {
    cluster.on(writeCommand)
  }
  
  def read[T](readCommand: ReadCommand): T = {
    cluster.on(readCommand).asInstanceOf[T]
  }
  
  def readLocal[T](readCommand: ReadCommand): T = {
    cluster.onLocal(readCommand).asInstanceOf[T]
  }
  
  def isLeader: Boolean = cluster.awaitLeader == cluster.local
  
  
}