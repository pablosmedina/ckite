package ckite.example
import ckite.CKiteBuilder

object KVStoreBootstrap extends App {

  val localBinding = System.getProperty("binding")
  val members = if (System.getProperty("members") != null)  System.getProperty("members").split(",").toSeq else Seq()
  val ckite = CKiteBuilder().withLocalBinding(localBinding)
  								.withMembersBindings(members)
  								.withDataDir(System.getProperty("dataDir"))
  								.withStateMachine(new KVStore())
  								.build()
  ckite start
}
