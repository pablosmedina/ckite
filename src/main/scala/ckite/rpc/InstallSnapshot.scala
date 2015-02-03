package ckite.rpc

import ckite.rlog.Snapshot
import ckite.rpc.LogEntry.Term

case class InstallSnapshot(term: Term, leaderId: String, snapshot: Snapshot)

case class InstallSnapshotResponse(success: Boolean)