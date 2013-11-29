package the.walrus.ckite.http

import the.walrus.ckite.Cluster
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Http
import java.net.InetSocketAddress

class HttpServer(cluster: Cluster) {
  
  def start() = {
    
     val server = ServerBuilder()
      .codec(RichHttp[Request](Http()))
      .bindTo(new InetSocketAddress(cluster.local.binding.split(":")(1).toInt + 1000))
      .name("restserver")
      .build(new HttpService(cluster))
    
  }
}