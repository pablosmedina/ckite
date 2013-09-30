package the.walrus.ckite.bootstrap.tomcat

import org.apache.coyote.http11.Http11Protocol
import java.util.concurrent.Executors
import java.io.File
import org.apache.catalina.startup.Tomcat
import scala.reflect._
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext
import org.springframework.web.servlet.DispatcherServlet
import the.walrus.ckite.bootstrap.spring.SpringWebConfigurer

class TomcatBootstrap(port: String) {

  val tomcat = new Tomcat();
  
  def start() = {

    val currentDir = new File(".").getCanonicalPath();
    val tomcatDir = currentDir + File.separatorChar + "tomcat";

    tomcat.setPort(Integer.valueOf(port))
    tomcat.setBaseDir(tomcatDir)
    
    configureThreadPool()
    configureSpring()
    
    tomcat.start()
    tomcat.getServer().await()
  }
  
  private def configureThreadPool()  = {
    val protocol = tomcat.getConnector().getProtocolHandler()
    protocol match {
      case pr: Http11Protocol => {
        val executor = Executors.newFixedThreadPool(200)
        pr.setExecutor(executor)
      }
    }
  }
  
  private def configureSpring(): Unit = {
    val ctx = tomcat.addWebapp("/", new File(".").getAbsolutePath())
    val servlet = tomcat.addServlet("/", "dispatcher", classOf[DispatcherServlet].getName())
    servlet.setLoadOnStartup(1)
    servlet.addInitParameter("contextClass", classOf[AnnotationConfigWebApplicationContext].getName())
    servlet.addInitParameter("contextConfigLocation",classOf[SpringWebConfigurer].getName())
    ctx.addServletMapping("/*", "dispatcher", true)
  }

}