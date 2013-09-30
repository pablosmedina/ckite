package the.walrus.ckite.bootstrap

import org.apache.catalina.startup.Tomcat
import java.io.File
import org.apache.catalina.connector.Connector
import org.apache.coyote.http11.Http11Protocol
import java.util.concurrent.Executors
import org.apache.naming.resources.VirtualDirContext
import org.springframework.web.servlet.DispatcherServlet
import java.util.Arrays
import the.walrus.ckite.bootstrap.tomcat.TomcatBootstrap

object CKite extends App {

  val tomcatBootstrap = new TomcatBootstrap(System.getProperty("port"))
  tomcatBootstrap.start()

}