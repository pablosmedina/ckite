package the.walrus.ckite.rpc.rest

import scala.util.Try
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.SerializableEntity
import java.io.Serializable
import org.apache.http.HttpResponse
import the.walrus.ckite.Member
import the.walrus.ckite.rpc.RequestVote
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.impl.conn.PoolingClientConnectionManager
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.Connector

class RestConnector extends Connector with Logging {

  val httpClient = new DefaultHttpClient(new PoolingClientConnectionManager())

  override def send(member: Member, request: RequestVote): Try[RequestVoteResponse] = {
    Try {
    	post(request, member.id, "requestVote")
    } map { response => deserialize[RequestVoteResponse](response) }
  }
  
  override def send(member: Member, appendEntries: AppendEntries): Try[AppendEntriesResponse] = {
	  Try {
		  post(appendEntries, member.id, "appendEntries")
	  } map { response => deserialize[AppendEntriesResponse](response) }
  }
  
  override def send(leader: Member, command: Command) = {
	  	post(command, leader.id, "forwardCommand")
  }
  
  private def post[T <: Serializable](request: T, memberId: String, message: String)  = {
	    val httpPost = new HttpPost(s"http://$memberId/ckite/$message")
	    httpPost.setEntity(new SerializableEntity(request, false))
		httpClient.execute(httpPost)
  }
  
  private def deserialize[T](response: HttpResponse) = {
	  val inputstream = response.getEntity().getContent()
      val deserializedResponse = Serializer.fromInputStream[T](inputstream)
      inputstream.close()
      deserializedResponse
  }

}