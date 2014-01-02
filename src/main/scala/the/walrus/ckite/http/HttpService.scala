package the.walrus.ckite.http

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
import the.walrus.ckite.Cluster
import the.walrus.ckite.RLog
import the.walrus.ckite.rpc.EnterJointConsensus
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import the.walrus.ckite.example.Get
import the.walrus.ckite.example.Put

class HttpService(cluster: Cluster) extends Service[Request, Response] {
  
  val futurePool = FuturePool(Executors.newFixedThreadPool(8))
  
  def apply(request: Request) = {
      request.method -> Path(request.path) match {
        case Method.Get -> Root / "rlog" => futurePool {
          val response = Response()
          response.contentString = cluster.rlog.toString
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
          cluster on Put(key, value)
          response.contentString = s"put[$key,$value]"
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