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

  def withMinElectionTimeout(minElectionTimeout: Int): CKiteBuilder = {
    configuration.withMinElectionTimeout(minElectionTimeout)
    this
  }

  def withMaxElectionTimeout(maxElectionTimeout: Int): CKiteBuilder = {
    configuration.withMaxElectionTimeout(maxElectionTimeout)
    this
  }

  def withHeartbeatsInterval(heartbeatsInterval: Int): CKiteBuilder = {
    configuration.withHeartbeatsInterval(heartbeatsInterval)
    this
  }

  def withLocalBinding(localBinding: String): CKiteBuilder = {
    configuration.withLocalBinding(localBinding)
    this
  }
  
  def withDataDir(dataDir: String): CKiteBuilder = {
    configuration.withDataDir(dataDir)
    this
  }

  def withMemberBindings(memberBindings: Seq[String]): CKiteBuilder = {
    configuration.withMemberBindings(memberBindings)
    this
  }
  
  def withMemberBindings(memberBindings: String): CKiteBuilder = {
    configuration.withMemberBindings(memberBindings.split(","))
    this
  }
  
  def withLogCompactionThreshold(threshold: Int): CKiteBuilder = {
    configuration.withLogCompactionThreshold(threshold)
    this
  }
  
  def withStateMachine(stateMachine: StateMachine): CKiteBuilder = {
    this.stateMachine = stateMachine
    this
  }
  
  def withStateMachine(stateMachine: ckite.statemachine.j.StateMachine): CKiteBuilder = {
    this.stateMachine = new StateMachineWrapper(stateMachine)
    this
  }

  def build(): CKite = {
    new CKite(new Cluster(stateMachine, configuration))
  }
  
}

object CKiteBuilder {
  
  def apply() = {
    new CKiteBuilder()
  }
  
}