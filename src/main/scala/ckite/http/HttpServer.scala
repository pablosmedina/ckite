package ckite.http

import ckite.Cluster
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import com.twitter.util.Closable

class HttpServer(cluster: Cluster) {
  
  var closed = false
  var server: Closable = _
  
  def start() = {
    val restServerPort = cluster.local.id.split(":")(1).toInt + 1000
    val adminServerPort = restServerPort + 1000
     server = ServerBuilder()
      .codec(RichHttp[Request](Http()))
      .bindTo(new InetSocketAddress(restServerPort))
      .name("HttpServer")
      .build(new HttpService(cluster))
  }
  
  def stop() = synchronized {
    if (!closed) {
    	server.close()
    	closed = true
    }
  }
  
}

object HttpServer {
  def apply(cluster: Cluster) = new HttpServer(cluster)
}
