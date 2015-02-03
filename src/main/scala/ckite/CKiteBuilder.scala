package ckite

import ckite.rlog.{ MemoryStorage, Storage }
import ckite.rpc.Rpc
import ckite.statemachine.StateMachine
import ckite.statemachine.j.StateMachineWrapper
import com.typesafe.config.ConfigFactory

class CKiteBuilder {

  private val configuration = new Configuration(ConfigFactory.load())
  private var stateMachine: StateMachine = _
  private var rpc: Rpc = _
  private var storage: Storage = MemoryStorage()

  def id(memberId: String): CKiteBuilder = {
    configuration.withId(memberId)
    CKiteBuilder.this
  }

  def minElectionTimeout(minElectionTimeout: Int): CKiteBuilder = {
    configuration.withMinElectionTimeout(minElectionTimeout)
    CKiteBuilder.this
  }

  def maxElectionTimeout(maxElectionTimeout: Int): CKiteBuilder = {
    configuration.withMaxElectionTimeout(maxElectionTimeout)
    CKiteBuilder.this
  }

  def heartbeatsPeriod(heartbeatsInterval: Int): CKiteBuilder = {
    configuration.withHeartbeatsInterval(heartbeatsInterval)
    CKiteBuilder.this
  }

  def listenAddress(localBinding: String): CKiteBuilder = {
    configuration.withLocalBinding(localBinding)
    CKiteBuilder.this
  }

  def members(memberBindings: Seq[String]): CKiteBuilder = {
    configuration.withMemberBindings(memberBindings)
    CKiteBuilder.this
  }

  def members(memberBindings: String): CKiteBuilder = {
    configuration.withMemberBindings(memberBindings.split(","))
    CKiteBuilder.this
  }

  def compactionThreshold(threshold: Int): CKiteBuilder = {
    configuration.withLogCompactionThreshold(threshold)
    CKiteBuilder.this
  }

  def stateMachine(stateMachine: StateMachine): CKiteBuilder = {
    CKiteBuilder.this.stateMachine = stateMachine
    CKiteBuilder.this
  }

  def stateMachine(stateMachine: ckite.statemachine.j.StateMachine): CKiteBuilder = {
    CKiteBuilder.this.stateMachine = new StateMachineWrapper(stateMachine)
    CKiteBuilder.this
  }

  def bootstrap(enabled: Boolean): CKiteBuilder = {
    configuration.bootstrap(enabled)
    CKiteBuilder.this
  }

  def rpc(rpcImpl: Rpc): CKiteBuilder = {
    rpc = rpcImpl
    CKiteBuilder.this
  }

  def storage(storageImpl: Storage): CKiteBuilder = {
    storage = storageImpl
    CKiteBuilder.this
  }

  def build: CKite = {
    if (rpc == null) throw new IllegalStateException("Rpc implementation is required")
    val cluster = new Raft(stateMachine, rpc, storage, configuration)
    new CKiteClient(cluster, rpc.createServer(cluster), CKiteBuilder.this)
  }

}

object CKiteBuilder {

  def apply() = {
    new CKiteBuilder()
  }

}