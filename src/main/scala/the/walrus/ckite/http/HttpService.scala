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
import the.walrus.ckite.rpc.Get
import the.walrus.ckite.rpc.Put

class HttpService(cluster: Cluster) extends Service[Request, Response] {
  def apply(request: Request) = {
      request.method -> Path(request.path) match {
        case Method.Get -> Root / "rlog" => Future.value {
          val response = Response()
          response.contentString = RLog.toString
          response
        }
        case Method.Get -> Root / "get" / key => Future.value {
          val response = Response()
          val result = cluster.onReadonly(Get(key))
          response.contentString = s"$result"
          response
        }
        case Method.Get -> Root / "put" / key / value => Future.value {
          val response = Response()
          cluster on Put(key, value)
          response.contentString = s"put[$key,$value]"
          response
        }
        case _ =>
          Future value Response(Http11, NotFound)
      }
  }
}