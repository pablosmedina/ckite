package the.walrus.ckite

class CKiteBuilder {

  var minElectionTimeout: Int = 1000
  var maxElectionTimeout: Int = 1250
  var heartbeatsInterval: Int = 250
  var localBinding: String = _
  var membersBindings: Seq[String]  = _

  def withMinElectionTimeout(minElectionTimeout: Int): CKiteBuilder = {
    this.minElectionTimeout = minElectionTimeout
    this
  }

  def withMaxElectionTimeout(maxElectionTimeout: Int): CKiteBuilder = {
    this.maxElectionTimeout = maxElectionTimeout
    this
  }

  def withHeartbeatsInterval(heartbeatsInterval: Int): CKiteBuilder = {
    this.heartbeatsInterval = heartbeatsInterval
    this
  }

  def withLocalBinding(localBinding: String): CKiteBuilder = {
    this.localBinding = localBinding
    this
  }

  def withMembersBindings(membersBindings: Seq[String]): CKiteBuilder = {
    this.membersBindings = membersBindings
    this
  }

  def build(): CKite = {
    new CKite(new Cluster(Configuration(localBinding, membersBindings, minElectionTimeout, maxElectionTimeout, heartbeatsInterval)))
  }

}

object CKiteBuilder {
  
  def apply() = {
    new CKiteBuilder()
  }
  
}