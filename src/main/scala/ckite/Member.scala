package ckite

import java.util.concurrent.atomic.AtomicInteger
import ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import ckite.states.Follower
import ckite.rpc.WriteCommand
import ckite.states.Leader
import ckite.states.Candidate
import ckite.rpc.RequestVoteResponse
import ckite.util.Logging
import ckite.states.State
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.AppendEntries
import ckite.rpc.Connector
import ckite.rpc.thrift.ThriftConnector
import java.net.ConnectException
import com.twitter.finagle.ChannelWriteException
import ckite.rpc.AppendEntriesResponse
import ckite.states.Starter
import ckite.rpc.JointConfiguration
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import ckite.states.Stopped
import java.util.concurrent.atomic.AtomicBoolean
import ckite.rlog.Snapshot
import scala.concurrent.Future

abstract class Member(binding: String) extends Logging {

  def id() = s"$binding"
  
  def forwardCommand[T](command: Command): Future[T]
  
  override def toString() = id
  
}