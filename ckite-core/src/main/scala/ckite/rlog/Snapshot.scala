package ckite.rlog

import java.nio.ByteBuffer

import ckite.ClusterConfiguration
import ckite.rpc.LogEntry._

case class Snapshot(term: Term, index: Index, clusterConfiguration: ClusterConfiguration, stateMachineSerialized: ByteBuffer)

