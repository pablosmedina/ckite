package ckite.rpc.thrift

import java.nio.ByteBuffer

import ckite.rlog.Snapshot
import ckite.rpc._
import ckite.util.Logging
import ckite.util.Serializer

object ThriftConverters extends Logging {

  implicit def requestVoteToThrift(request: RequestVote): RequestVoteST = {
    RequestVoteST(request.memberId, request.term, request.lastLogIndex, request.lastLogTerm)
  }

  implicit def requestVoteFromThrift(requestVote: RequestVoteST): RequestVote = {
    RequestVote(requestVote.memberId, requestVote.term, requestVote.lastLogIndex, requestVote.lastLogTerm)
  }

  implicit def appendEntriesToThrift(request: AppendEntries): AppendEntriesST = {
    val entries: Seq[LogEntryST] = request.entries.map(entry ⇒ logEntryToThrift(entry)).toSeq
    AppendEntriesST(request.term, request.leaderId, request.commitIndex, request.prevLogIndex, request.prevLogTerm, Some(entries))
  }

  implicit def appendEntriesFromThrift(request: AppendEntriesST): AppendEntries = {
    val entries = request.entries.get.map(entry ⇒ logEntryFromThrift(entry)).toList
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

  implicit def snapshotToThrift(snapshot: Snapshot): SnapshotST = {
    val bb2: ByteBuffer = snapshot.clusterConfiguration
    val bb: ByteBuffer = snapshot.stateMachineSerialized
    SnapshotST(bb, snapshot.index, snapshot.term, bb2)
  }

  implicit def snapshotFromThrift(snapshotST: SnapshotST): Snapshot = {
    Snapshot(snapshotST.lastLogEntryTerm, snapshotST.lastLogEntryIndex, snapshotST.membershipState, snapshotST.stateMachineState)
  }

  implicit def installSnapshotToThrift(installSnapshot: InstallSnapshot): InstallSnapshotST = {
    InstallSnapshotST(installSnapshot.term, installSnapshot.leaderId, installSnapshot.snapshot)
  }

  implicit def installSnapshotFromThrift(installSnapshotST: InstallSnapshotST): InstallSnapshot = {
    InstallSnapshot(installSnapshotST.term, installSnapshotST.leaderId, installSnapshotST.snapshot)
  }

  implicit def installSnapshotResponseFromThrift(installSnapshotResponseST: InstallSnapshotResponseST): InstallSnapshotResponse = {
    InstallSnapshotResponse(installSnapshotResponseST.success)
  }

  implicit def installSnapshotResponseToThrift(installSnapshotResponse: InstallSnapshotResponse): InstallSnapshotResponseST = {
    InstallSnapshotResponseST(installSnapshotResponse.success)
  }

  implicit def joinMemberToThrift(joinRequest: JoinMember): JoinMemberST = {
    JoinMemberST(joinRequest.memberId)
  }

  implicit def joinMemberResponseToThrift(joinResponse: JoinMemberResponse): JoinMemberResponseST = {
    JoinMemberResponseST(joinResponse.success)
  }

  implicit def joinMemberResponseFromThrift(joinResponse: JoinMemberResponseST): JoinMemberResponse = {
    JoinMemberResponse(joinResponse.success)
  }

}