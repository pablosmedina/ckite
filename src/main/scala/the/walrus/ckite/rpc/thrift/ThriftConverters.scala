package the.walrus.ckite.rpc.thrift

import java.nio.ByteBuffer

import the.walrus.ckite.rlog.Snapshot
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.GetMembersResponse
import the.walrus.ckite.rpc.JoinRequest
import the.walrus.ckite.rpc.JoinResponse
import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.util.Logging
import the.walrus.ckite.util.Serializer

object ThriftConverters extends Logging {

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
    LogEntryST(entry.term, entry.index, entry.command)
  } 
  
  implicit def logEntryFromThrift(entry: LogEntryST): LogEntry = {
    LogEntry(entry.term, entry.index, entry.command)
  } 
  
  implicit def anyToThrift[T](command: T): ByteBuffer = {
    val bb = ByteBuffer.wrap(Serializer.serialize(command))
    bb
  } 
  
  implicit def anyFromThrift[T](byteBuffer: ByteBuffer): T = {
    val remaining = byteBuffer.remaining()
    val bytes = new Array[Byte](remaining)
    byteBuffer.get(bytes)
    val c = Serializer.deserialize[T](bytes)
    c
  } 
  
  implicit def snapshotToThrift(snapshot: Snapshot): InstallSnapshotST = {
	val bb2:ByteBuffer = snapshot.membership
    val bb:ByteBuffer = snapshot.stateMachineState
    InstallSnapshotST(bb, snapshot.lastLogEntryIndex, snapshot.lastLogEntryTerm,bb2)
  }
  
  implicit def snapshotFromThrift(installSnapshotST: InstallSnapshotST): Snapshot = {
    new Snapshot(installSnapshotST.stateMachineState, installSnapshotST.lastLogEntryIndex, installSnapshotST.lastLogEntryTerm, installSnapshotST.membershipState)
  }
  
  
  implicit def joinRequestToThrift(joinRequest: JoinRequest): JoinRequestST = {
    JoinRequestST(joinRequest.joiningMemberId)
  }
  
  implicit def joinResponseFromThrift(joinResponse: JoinResponseST): JoinResponse = {
    JoinResponse(joinResponse.success)
  }
  
  implicit def getMembersResponseFromThrift(getMembersResponse: GetMembersResponseST): GetMembersResponse = {
    GetMembersResponse(getMembersResponse.success, getMembersResponse.members)
  }
  
}