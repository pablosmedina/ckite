package the.walrus.ckite.http

import the.walrus.ckite.Cluster
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import com.twitter.util.Closable
import com.twitter.finagle.http.HttpMuxer

class HttpServer(cluster: Cluster) {
  
  var server: Closable = _
  var adminServer: Closable = _
  
  def start() = {
    val restServerPort = cluster.local.binding.split(":")(1).toInt + 1000
    val adminServerPort = restServerPort + 1000
     server = ServerBuilder()
      .codec(RichHttp[Request](Http()))
      .bindTo(new InetSocketAddress(restServerPort))
      .name("HttpServer")
      .build(new HttpService(cluster))
     adminServer = com.twitter.finagle.Http.serve(s":$adminServerPort", HttpMuxer)
  }
  
  def stop() = {
    server.close()
    adminServer.close()
  }
  
}

object HttpServer {
  def apply(cluster: Cluster) = new HttpServer(cluster)
}
