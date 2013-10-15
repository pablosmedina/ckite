package the.walrus.ckite.rpc.thrift

import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries
import java.nio.ByteBuffer
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.rpc.Command

object ThriftConverters {

  implicit def requestVoteToThrift(request: RequestVote) : RequestVoteST = {
     RequestVoteST(request.memberId, request.term, request.lastLogIndex, request.lastLogTerm)
  }
  
  implicit def requestVoteFromThrift(requestVote: RequestVoteST) : RequestVote = {
    RequestVote(requestVote.memberId, requestVote.term, requestVote.lastLogIndex, requestVote.lastLogTerm)
  }
  
  implicit def appendEntriesToThrift(request: AppendEntries) : AppendEntriesST = {
    val entries:Seq[LogEntryST] = request.entries.map( entry => logEntryToThrift(entry)).toSeq
    AppendEntriesST(request.term, request.leaderId, request.commitIndex, request.prevLogIndex, request.prevLogTerm, Some(entries) )
  }
  
  implicit def appendEntriesFromThrift(request: AppendEntriesST) : AppendEntries = {
    val entries =  request.entries.get.map(entry => logEntryFromThrift(entry)).toList
    AppendEntries(request.term, request.leaderId, request.commitIndex, request.prevLogIndex, request.prevLogTerm, entries)
  } 
  
  implicit def requestVoteResponseToThrift(response: RequestVoteResponse): RequestVoteResponseST = {
    RequestVoteResponseST(response.currentTerm, response.granted)
  }
  
  implicit def requestVoteResponseFromThrift(response: RequestVoteResponseST): RequestVoteResponse = {
    RequestVoteResponse(response.currentTerm, response.granted)
  }
  
  implicit def appendEntriesResponseFromThrift(response: AppendEntriesResponseST): AppendEntriesResponse = {
    AppendEntriesResponse(response.term, response.success)
  }
  
  implicit def appendEntriesResponseToThrift(response: AppendEntriesResponse): AppendEntriesResponseST = {
    AppendEntriesResponseST(response.term, response.success)
  }
  
  implicit def logEntryToThrift(entry: LogEntry): LogEntryST = {
    LogEntryST(entry.term, entry.index, null.asInstanceOf[ByteBuffer])
  } 
  
  implicit def logEntryFromThrift(entry: LogEntryST): LogEntry = {
    LogEntry(entry.term, entry.index, null.asInstanceOf[Command])
  } 
  
}