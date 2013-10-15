package the.walrus.ckite

case class Configuration(localBinding: String, membersBindings: Seq[String], 
						minElectionTimeout: Int, maxElectionTimeout: Int, heartbeatsInterval: Int)