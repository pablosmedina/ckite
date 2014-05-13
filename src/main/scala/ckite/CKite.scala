package ckite

import ckite.rpc.thrift.ThriftServer
import ckite.rpc.WriteCommand
import ckite.rpc.ReadCommand
import java.util.concurrent.atomic.AtomicBoolean

class CKite(private[ckite] val cluster: Cluster, private[ckite] val builder: CKiteBuilder) {

  private val thrift = ThriftServer(cluster)
  private val stopped = new AtomicBoolean(false)
  
  def start = {
    thrift start

    cluster start
  }

  def stop = {
    if (!stopped.getAndSet(true)) {
    	thrift stop
    	
    	cluster stop
    }
  }

  def write[T](writeCommand: WriteCommand): T = cluster.on[T](writeCommand)

  def read[T](readCommand: ReadCommand): T = cluster.on[T](readCommand)

  def readLocal[T](readCommand: ReadCommand): T = cluster.onLocal(readCommand).asInstanceOf[T]

  def addMember(memberBinding: String) = cluster.addMember(memberBinding)

  def removeMember(memberBinding: String) = cluster.removeMember(memberBinding)

  def isLeader: Boolean = cluster.isLeader
  
  def getMembers:List[String] = cluster.getMembers.toList
  
  def status = cluster.getStatus

}