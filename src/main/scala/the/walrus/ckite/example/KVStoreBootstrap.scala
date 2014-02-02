package the.walrus.ckite.example
import the.walrus.ckite.CKiteBuilder

object KVStoreBootstrap extends App {

  val localPort = System.getProperty("port")
  val members = if (System.getProperty("members") != null)  System.getProperty("members").split(",").toSeq else Seq()
  val ckite = CKiteBuilder().withLocalBinding(s"localhost:$localPort")
  								.withMembersBindings(members)
  								.withDataDir(System.getProperty("dataDir"))
  								.withStateMachine(new KVStore())
  								.build()
  ckite start
}
