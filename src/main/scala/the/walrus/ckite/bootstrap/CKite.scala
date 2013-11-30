package the.walrus.ckite.bootstrap

import java.io.File
import java.util.concurrent.Executors
import java.util.Arrays
import the.walrus.ckite.CKiteBuilder

object CKite extends App {

  val localPort = System.getProperty("port")
  val ckite = CKiteBuilder().withLocalBinding(s"localhost:$localPort")
  								.withMembersBindings(System.getProperty("members").split(",").toSeq)
  								.build()
  ckite start
}