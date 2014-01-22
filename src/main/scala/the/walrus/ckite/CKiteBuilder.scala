package the.walrus.ckite

import the.walrus.ckite.statemachine.StateMachine
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.typesafe.config.impl.ConfigInt
import com.typesafe.config.ConfigValueFactory

class CKiteBuilder {

  val configuration = new Configuration(ConfigFactory.load("ckite-defaults.conf"))
  var stateMachine: StateMachine = _

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

  def withMembersBindings(membersBindings: Seq[String]): CKiteBuilder = {
    configuration.withMembersBindings(membersBindings)
    this
  }
  
  def withStateMachine(stateMachine: StateMachine): CKiteBuilder = {
    this.stateMachine = stateMachine
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