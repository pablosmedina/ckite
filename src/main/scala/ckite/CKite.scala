package ckite

import ckite.rpc.thrift.ThriftServer
import ckite.http.HttpServer
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand

class CKite(cluster: Cluster) {

  private val thrift = ThriftServer(cluster)
  private val http = HttpServer(cluster)
  
  def start() = {
    
    http start
    
    thrift start
    
    cluster start
    
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
  
  def addMember(memberBinding: String) = {
    cluster.addMember(memberBinding)
  }
  
  def decomission() = {
    cluster.removeMember(cluster.local.id)
  }
  
  def removeMember(memberBinding: String) = {
    cluster.removeMember(memberBinding)
  }
  
  def isLeader: Boolean = cluster.awaitLeader == cluster.local
  
  
}