package the.walrus.ckite.rpc.thrift

import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries
import java.nio.ByteBuffer
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.rpc.WriteCommand
import java.nio.HeapByteBuffer
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rlog.Snapshot

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
  
  implicit def anyToThrift(command: Any): ByteBuffer = {
    val bb = ByteBuffer.wrap(serialize(command))
    bb
  } 
  
  implicit def commandFromThrift[T](byteBuffer: ByteBuffer): T = {
    val remaining = byteBuffer.remaining()
    val bytes = new Array[Byte](remaining)
    byteBuffer.get(bytes)
    val c = deserialize[T](bytes)
    c
  } 
  
  implicit def snapshotToThrift(snapshot: Snapshot): InstallSnapshotST = {
    InstallSnapshotST(snapshot.stateMachineState, snapshot.lastLogEntryIndex, snapshot.lastLogEntryTerm)
  }
  
  implicit def snapshotFromThrift(installSnapshotST: InstallSnapshotST): Snapshot = {
    new Snapshot(installSnapshotST.stateMachineState, installSnapshotST.lastLogEntryIndex, installSnapshotST.lastLogEntryTerm)
  } 
  
  private def serialize(command: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(command)
    oos.flush()
    baos.flush()
    val bytes = baos.toByteArray()
    oos.close()
    bytes
  }
  
  private def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val command = ois.readObject().asInstanceOf[T]
    ois.close()
    command
  }
  
}