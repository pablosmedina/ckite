package ckite.http

import com.twitter.finagle.Service
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.path.Path
import com.twitter.util.Future
import com.twitter.finagle.http.path.Root
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import com.twitter.finagle.http.{ Http, RichHttp, Request, Response }
import com.twitter.finagle.http.Status._
import com.twitter.finagle.http.Version.Http11
import com.twitter.finagle.http.path._
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.finagle.builder.{ Server, ServerBuilder }
import ckite.Cluster
import ckite.RLog
import ckite.rpc.EnterJointConsensus
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import ckite.example.Get
import ckite.example.Put
import com.twitter.server.TwitterServer
import com.twitter.finagle.http.HttpMuxer
import com.twitter.server.util.JsonConverter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class HttpService(cluster: Cluster) extends Service[Request, Response] with TwitterServer {

  val futurePool = FuturePool(Executors.newFixedThreadPool(8))
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  val printer = new DefaultPrettyPrinter
  printer.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
  val writer = mapper.writer(printer)

  def apply(request: Request) = {
    request.method -> Path(request.path) match {
      case Method.Get -> Root / "status" => futurePool {
        val response = Response()
        val clusterStatus = ClusterStatus(cluster.local.term, cluster.local.currentState.toString(), cluster.local.currentState.info())
        val logStatus = LogStatus(cluster.rlog.size, cluster.rlog.commitIndex.intValue(), cluster.rlog.getLastLogEntry)
        val status = Status(clusterStatus, logStatus)
        response.contentString = writer.writeValueAsString(status)
        response
      }
      case Method.Get -> Root / "get" / key => futurePool {
        val response = Response()
        val result = cluster.onLocal(Get(key))
        response.contentString = s"$result"
        response
      }
      case Method.Get -> Root / "put" / key / value => futurePool {
        Thread.currentThread().setName("Command")
        val response = Response()
        val result = cluster.on[String](Put(key, value))
        response.contentString = result
        response
      }
      case Method.Get -> Root / "changecluster" / bindings => futurePool {
        Thread.currentThread().setName("ClusterMembershipChange")
        val response = Response()
        cluster on EnterJointConsensus(bindings.split(",").toList)
        response.contentString = s"ClusterMembershipChange ok"
        response
      }
      case _ =>
        Future value Response(Http11, NotFound)
    }
  }
}