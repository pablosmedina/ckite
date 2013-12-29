package the.walrus.ckite.http

import the.walrus.ckite.Cluster
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import com.twitter.util.Closable

class HttpServer(cluster: Cluster) {
  
  var server: Closable = _
  
  def start() = {
     server = ServerBuilder()
      .codec(RichHttp[Request](Http()))
      .bindTo(new InetSocketAddress(cluster.local.binding.split(":")(1).toInt + 1000))
      .name("restserver")
      .build(new HttpService(cluster))
  }
  
  def stop() = server.close()
  
}

object HttpServer {
  def apply(cluster: Cluster) = new HttpServer(cluster)
}
