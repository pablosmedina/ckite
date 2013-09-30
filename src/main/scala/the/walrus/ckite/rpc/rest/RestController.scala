package the.walrus.ckite.rpc.rest

import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.http.HttpStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.http.HttpEntity
import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.RequestVote
import org.slf4j.MDC
import org.springframework.web.bind.annotation.PathVariable
import the.walrus.ckite.rpc.Put
import org.springframework.stereotype.Component
import org.springframework.context.annotation.DependsOn
import javax.annotation.PostConstruct
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.Get
import the.walrus.ckite.RLog

@Controller
@DependsOn(Array("CKiteSpringBootstrap"))
@RequestMapping(Array("/ckite"))
class RestController {

  @Autowired
  var cluster : Cluster = null
  
  private def contextInfo(name: String) = {
    Thread.currentThread().setName(name)
    cluster.updateContextInfo()
  }
  
  @RequestMapping(value=Array("requestVote"), method = Array(RequestMethod.POST))
  @ResponseStatus(HttpStatus.OK)
  def requestVote(requestEntity: HttpEntity[Array[Byte]]): ResponseEntity[Array[Byte]] = {
	 contextInfo("requestVoteRequest")
     val response = cluster.onMemberRequestingVote(Serializer.fromByteArray[RequestVote](requestEntity.getBody()))
     serializedResponse(response)
  }
  
  @RequestMapping(value=Array("appendEntries"),method = Array(RequestMethod.POST))
  @ResponseStatus(HttpStatus.OK)
  def appendEntries(requestEntity: HttpEntity[Array[Byte]]): ResponseEntity[Array[Byte]] = {
    contextInfo("appendEntriesRequest")
    val response = cluster.onAppendEntriesReceived(Serializer.fromByteArray[AppendEntries](requestEntity.getBody()))
    serializedResponse(response)
  }
  
  @RequestMapping(value=Array("kvstore/{key}/{value}"),method = Array(RequestMethod.PUT))
  @ResponseStatus(HttpStatus.OK)
  def update(@PathVariable("key") key: String, @PathVariable("value") value: String) = {
      contextInfo("kvStorePut")
	  val put = Put[String, String](key,value)
      cluster.onCommandReceived(put)
  }
  
  @RequestMapping(value=Array("kvstore/{key}"),method = Array(RequestMethod.GET))
  @ResponseStatus(HttpStatus.OK)
  def get(@PathVariable("key") key: String): ResponseEntity[String] = {
     contextInfo("kvStoreGet")
	  val get = Get[String](key)
      new ResponseEntity[String](cluster.onQueryReceived(get).toString(), HttpStatus.OK)
  }
  
  @RequestMapping(value=Array("rlog"),method = Array(RequestMethod.GET))
  @ResponseStatus(HttpStatus.OK)
  def dumpRLog(): ResponseEntity[String] = {
      new ResponseEntity[String](RLog.toString, HttpStatus.OK)
  }
  
  private def serializedResponse[T](response: T): ResponseEntity[Array[Byte]] = {
	  new ResponseEntity(Serializer.toByteArray(response), HttpStatus.OK)
  }
  
}

