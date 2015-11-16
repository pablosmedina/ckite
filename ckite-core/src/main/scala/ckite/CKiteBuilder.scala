package ckite

import ckite.rlog.Storage
import ckite.rpc.Rpc
import ckite.statemachine.StateMachine
import ckite.statemachine.j.StateMachineWrapper
import ckite.storage.MemoryStorage
import com.typesafe.config.ConfigFactory

class CKiteBuilder {

  private val configuration = new Configuration(ConfigFactory.load())
  private var stateMachine: Option[StateMachine] = None
  private var rpc: Option[Rpc] = None
  private var storage: Storage = MemoryStorage()

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
    CKiteBuilder.this.stateMachine = Some(stateMachine)
    CKiteBuilder.this
  }

  def stateMachine(stateMachine: ckite.statemachine.j.StateMachine): CKiteBuilder = {
    CKiteBuilder.this.stateMachine = Some(new StateMachineWrapper(stateMachine))
    CKiteBuilder.this
  }

  def bootstrap(enabled: Boolean): CKiteBuilder = {
    configuration.bootstrap(enabled)
    CKiteBuilder.this
  }

  def rpc(someRpc: Rpc): CKiteBuilder = {
    rpc = Some(someRpc)
    CKiteBuilder.this
  }

  def storage(someStorage: Storage): CKiteBuilder = {
    storage = someStorage
    CKiteBuilder.this
  }

  private def configuredStateMachine() = {
    stateMachine.getOrElse(throw new IllegalStateException("StateMachine required"))
  }

  private def configuredRpc() = {
    rpc.getOrElse(throw new IllegalStateException("RPC required"))
  }

  def build: CKite = {
    val stateMachine = configuredStateMachine()
    val rpc = configuredRpc()
    val raft = Raft(stateMachine, rpc, storage, configuration)
    CKiteClient(raft, rpc.createServer(raft, configuration.config), CKiteBuilder.this)
  }

}

object CKiteBuilder {
  def apply() = new CKiteBuilder()
}