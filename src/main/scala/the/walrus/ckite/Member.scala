package the.walrus.ckite

import java.util.concurrent.atomic.AtomicInteger
import the.walrus.ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import the.walrus.ckite.states.Follower
import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.states.Leader
import the.walrus.ckite.states.Candidate
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.util.Logging
import the.walrus.ckite.states.State
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.Connector
import the.walrus.ckite.rpc.thrift.ThriftConnector
import java.net.ConnectException
import com.twitter.finagle.ChannelWriteException
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.states.Starter
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.states.Stopped
import java.util.concurrent.atomic.AtomicBoolean
import the.walrus.ckite.rlog.Snapshot

abstract class Member(binding: String) extends Logging {

  def id() = s"$binding"
  
  def forwardCommand[T](command: Command): T
  
  override def toString() = id
  
}