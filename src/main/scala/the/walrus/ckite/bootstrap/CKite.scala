package the.walrus.ckite.bootstrap

import java.io.File
import java.util.concurrent.Executors
import java.util.Arrays
import the.walrus.ckite.CKiteBuilder

object CKite extends App {

  val localPort = System.getProperty("port")
  val members = if (System.getProperty("members") != null)  System.getProperty("members").split(",").toSeq else Seq()
  val ckite = CKiteBuilder().withLocalBinding(s"localhost:$localPort")
  								.withMembersBindings(members)
  								.build()
  ckite start
}