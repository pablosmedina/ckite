package ckite.rpc

import ckite.rlog.Snapshot

case class InstallSnapshot(snapshot: Snapshot)

case class InstallSnapshotResponse(success: Boolean)