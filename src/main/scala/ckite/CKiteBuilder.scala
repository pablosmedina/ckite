package ckite

import ckite.statemachine.StateMachine
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.typesafe.config.impl.ConfigInt
import com.typesafe.config.ConfigValueFactory
import ckite.statemachine.j.StateMachineWrapper

class CKiteBuilder {

  private val configuration = new Configuration(ConfigFactory.load())
  private var stateMachine: StateMachine = _

  def minElectionTimeout(minElectionTimeout: Int): CKiteBuilder = {
    configuration.withMinElectionTimeout(minElectionTimeout)
    this
  }

  def maxElectionTimeout(maxElectionTimeout: Int): CKiteBuilder = {
    configuration.withMaxElectionTimeout(maxElectionTimeout)
    this
  }

  def heartbeatsPeriod(heartbeatsInterval: Int): CKiteBuilder = {
    configuration.withHeartbeatsInterval(heartbeatsInterval)
    this
  }

  def listenAddress(localBinding: String): CKiteBuilder = {
    configuration.withLocalBinding(localBinding)
    this
  }
  
  def dataDir(dataDir: String): CKiteBuilder = {
    configuration.withDataDir(dataDir)
    this
  }

  def members(memberBindings: Seq[String]): CKiteBuilder = {
    configuration.withMemberBindings(memberBindings)
    this
  }
  
  def members(memberBindings: String): CKiteBuilder = {
    configuration.withMemberBindings(memberBindings.split(","))
    this
  }
  
  def compactionThreshold(threshold: Int): CKiteBuilder = {
    configuration.withLogCompactionThreshold(threshold)
    this
  }
  
  def stateMachine(stateMachine: StateMachine): CKiteBuilder = {
    this.stateMachine = stateMachine
    this
  }
  
  def stateMachine(stateMachine: ckite.statemachine.j.StateMachine): CKiteBuilder = {
    this.stateMachine = new StateMachineWrapper(stateMachine)
    this
  }
  
  def flushSize(flushSize: Long): CKiteBuilder = {
    configuration.withFlushSize(flushSize)
    this
  }
  
  def sync(enabled: Boolean):CKiteBuilder = {
    configuration.withSyncEnabled(enabled)
    this
  }
  
  def bootstrap(enabled: Boolean): CKiteBuilder = {
    configuration.bootstrap(enabled)
    this
  }

  def build(): CKite = {
    new CKite(new Cluster(stateMachine, configuration), this)
  }
  
}

object CKiteBuilder {
  
  def apply() = {
    new CKiteBuilder()
  }
  
}