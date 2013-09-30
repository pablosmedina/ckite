package the.walrus.ckite.bootstrap.spring

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import the.walrus.ckite.Cluster
import the.walrus.ckite.Member

@Component
class CKiteSpringBootstrap() {

  @Autowired
  var applicationContext: ApplicationContext = null

  @Autowired
  var beanFactory: DefaultListableBeanFactory = null

  @PostConstruct
  def init() = {
    start(System.getProperty("port"), System.getProperty("members").split(",").toList)
  }

  private def start(self: String, members: List[String]) = {
    getCluster(self, members).start()
  }

  private def getCluster(local: String, members: List[String]): Cluster = {
    val cluster = new Cluster(new Member("localhost", local), members map { _.split(":") } map { (array => new Member(array(0), array(1))) })
    beanFactory.registerSingleton("Cluster", cluster)
    cluster
  }

}