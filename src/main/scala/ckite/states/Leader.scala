package ckite.states

import java.util.concurrent.Executors
import java.util.concurrent.Future
import scala.util.Try
import ckite.Cluster
import ckite.rpc.WriteCommand
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import ckite.rpc.RequestVoteResponse
import ckite.util.Logging
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.RequestVote
import ckite.rpc.AppendEntries
import ckite.rpc.LogEntry
import ckite.RLog
import java.lang.Boolean
import ckite.Member
import ckite.rpc.EnterJointConsensus
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.LeaveJointConsensus
import ckite.exception.WriteTimeoutException
import ckite.util.CKiteConversions._
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.RemoteMember
import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.collection.mutable.ArrayBuffer
import ckite.rpc.NoOp
import ckite.rpc.NoOp
import scala.collection.JavaConverters._
import java.util.concurrent.TimeoutException
import scala.concurrent._
import scala.concurrent.duration._

/**
 * 	•! Initialize nextIndex for each to last log index + 1
 * •! Send initial empty AppendEntries RPCs (heartbeat) to each
 * follower; repeat during idle periods to prevent election
 * timeouts (§5.2)
 * •! Accept commands from clients, append new entries to local
 * log (§5.3)
 * •! Whenever last log index ! nextIndex for a follower, send
 * AppendEntries RPC with log entries starting at nextIndex,
 * update nextIndex if successful (§5.3)
 * •! If AppendEntries fails because of log inconsistency,
 * decrement nextIndex and retry (§5.3)
 * •! Mark entries committed if stored on a majority of servers
 * and some entry from current term is stored on a majority of
 * servers. Apply newly committed entries to state machine.
 * •! Step down if currentTerm changes (§5.5)
 */
class Leader(cluster: Cluster) extends State {

  val ReplicationTimeout = cluster.configuration.appendEntriesTimeout
  val startTime = System.currentTimeMillis()
  
  val heartbeater = new Heartbeater(cluster)
  val followersInfo = new ConcurrentHashMap[String, Long]()
  
  val appendEntriesTimeout = cluster.configuration.appendEntriesTimeout millis
  
  override def begin(term: Int) = {
    if (term < cluster.local.term) {
      LOG.debug(s"Cant be a Leader of term $term. Current term is ${cluster.local.term}")
      cluster.local.becomeFollower(cluster.local.term)
    } else {
      resetLastLog
      resetNextAndMatchIndexes
      resetFollowerInfo
      heartbeater start term
      LOG.debug("Append a NoOp as part of Leader initialization")
      on[Unit](NoOp())
      cluster.updateLeader(cluster.local.id)
      LOG.info(s"Start being Leader")
    }
  }
  
  private def resetLastLog = cluster.rlog.resetLastLog()

  private def resetNextAndMatchIndexes = {
    val nextIndex = cluster.rlog.lastLog.intValue() + 1
    cluster.membership.remoteMembers.foreach { member => member.setNextLogIndex(nextIndex); member.resetMatchIndex }
  }

  override def stop = {
    LOG.debug("Stop being Leader")
    heartbeater stop
    
    cluster.setNoLeader
  }

  override def on[T](command: Command): T = {
//    try {
	    command match {
	      case w: WriteCommand => onWriteCommand[T](w)
	      case r: ReadCommand => onReadCommand[T](r)
	    }
//    } catch {
//      case e: Exception => LOG.error("error", e); throw new RuntimeException(e)
//    }
  }

  private def onWriteCommand[T](write: WriteCommand): T = {
//    LOG.debug(s"Will wait for a majority consisting of ${cluster.membership.majoritiesMap} until $ReplicationTimeout ms")
    val (logEntry,promise) = cluster.rlog.append(write).asInstanceOf[(LogEntry,Promise[T])]
    replicate(logEntry)
    await(promise, logEntry)
  }
  
  private def await[T](promise: Promise[T], logEntry: LogEntry) = {
    try {
      val value = Await.result(promise.future, appendEntriesTimeout)
//      LOG.trace(s"Finish wait for $logEntry and got value $value")
      value
    } catch {
      case e: TimeoutException => {
        LOG.warn(s"WriteTimeout - $logEntry")
        throw new WriteTimeoutException(logEntry)
      }
    }
  }
  
  private def replicate(logEntry: LogEntry) = {
    if (cluster.hasRemoteMembers) {
      cluster.broadcastAppendEntries(logEntry.term)
    } else {
//      LOG.debug(s"No member to replicate")
      cluster.rlog commit logEntry.index
    }
  }
  
  
  private def onReadCommand[T](command: ReadCommand): T = {
    (cluster.rlog execute command).asInstanceOf[T]
  }

  override def on(appendEntries: AppendEntries): AppendEntriesResponse = {
    if (appendEntries.term < cluster.local.term) {
      AppendEntriesResponse(cluster.local.term, false)
    } else {
      stepDown(appendEntries.term, Some(appendEntries.leaderId))
      cluster.local on appendEntries
    }
  }

  override def on(requestVote: RequestVote): RequestVoteResponse = {
    if (requestVote.term <= cluster.local.term) {
      RequestVoteResponse(cluster.local.term, false)
    } else {
      stepDown(requestVote.term, None)
      cluster.local on requestVote
    }
  }

  override def on(jointConsensusCommited: MajorityJointConsensus) = {
    LOG.debug(s"Sending LeaveJointConsensus")
    cluster.on[Boolean](LeaveJointConsensus(jointConsensusCommited.newBindings))
  }
  
  override protected def getCluster: Cluster = cluster

  override def toString = "Leader"

  override def info(): StateInfo = {
    val now = System.currentTimeMillis()
    val followers = followersInfo.map {
      tuple =>
        val member = cluster.obtainRemoteMember(tuple._1).get
        (tuple._1, FollowerInfo(lastAck(tuple._2, now), member.matchIndex.intValue(), member.nextLogIndex.intValue()))
    }
    LeaderInfo(leaderUptime.toString, followers.toMap)
  }
  
  private def leaderUptime = System.currentTimeMillis() - startTime millis
  
  private def lastAck(ackTime: Long, now: Long) = if (ackTime > 0) (now - ackTime millis).toString else "Never"
  
  private def resetFollowerInfo = {
    cluster.membership.remoteMembers.foreach { member => followersInfo.put(member.id, -1) }
  }
  
  override def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {
      val time = System.currentTimeMillis() 
	  if (!request.entries.isEmpty) {
            onAppendEntriesResponseUpdateNextLogIndex(member, request, response)
       }
      val nextIndex = member.nextLogIndex.intValue()
       if (isLogEntryInSnapshot(nextIndex)) {
          val wasEnabled = member.disableReplications()
          if (wasEnabled) { 
        	LOG.debug(s"Next LogIndex #$nextIndex to be sent to ${member} is contained in a Snapshot. An InstallSnapshot will be sent.")
            sendSnapshot(member)
          }
      }
      followersInfo.put(member.id, time)
  }

  private def onAppendEntriesResponseUpdateNextLogIndex(member: RemoteMember, appendEntries: AppendEntries, appendEntriesResponse: AppendEntriesResponse) = {
    val lastEntrySent = appendEntries.entries.last.index
    if (appendEntriesResponse.success) {
      member.ackLogEntry(lastEntrySent)
      LOG.debug(s"Member ${member} ack - LogIndex sent #$lastEntrySent - next LogIndex is #${member.nextLogIndex}")
      tryToCommitEntries(lastEntrySent)
    } else {
      member.decrementNextLogIndex()
      LOG.debug(s"Member ${member} reject - LogIndex sent #$lastEntrySent - next LogIndex is #${member.nextLogIndex}")
    }
  }
  
  private def tryToCommitEntries(lastEntrySent: Long) = {
     val currentCommitIndex = cluster.rlog.commitIndex.longValue()
      (currentCommitIndex + 1) to lastEntrySent foreach { index =>
        if (reachMajority(index)) {
          cluster.rlog commit index
        }
    }
  }

  private def reachMajority(index: Long) = cluster.membership.reachMajority(membersHavingAtLeast(index) :+ cluster.local)
  
  private def membersHavingAtLeast(index: Long): Seq[RemoteMember] = {
    cluster.membership.remoteMembers.filter { remoteMember => remoteMember.matchIndex.longValue() >= index }
  }
  
  private def isLogEntryInSnapshot(logIndex: Int): Boolean = {
    cluster.rlog.snapshotManager.isInSnapshot(logIndex)
  }

  def sendSnapshot(member: RemoteMember) = {
    cluster.rlog.snapshotManager.latestSnapshot map { snapshot =>
      LOG.debug(s"Sending InstallSnapshot to ${member} containing $snapshot")
      member.sendSnapshot(snapshot).map { success =>
        cluster.inContext {
          if (success) {
            LOG.debug("Succesful InstallSnapshot")
            member.ackLogEntry(snapshot.lastLogEntryIndex)
            tryToCommitEntries(snapshot.lastLogEntryIndex)
          } else {
            LOG.debug("Failed InstallSnapshot")
          }
          member.enableReplications()
        }
      }

    }
  }
}


class Heartbeater(cluster: Cluster) extends Logging {

  val scheduledHeartbeatsPool = new ScheduledThreadPoolExecutor(1, new NamedPoolThreadFactory("Heartbeater", true))
  
  def start(term: Int) = {
    LOG.debug("Start Heartbeater")

    val task:Runnable = () => {
      cluster updateContextInfo

      LOG.trace("Heartbeater running")
      cluster.broadcastAppendEntries(term)
    } 
    scheduledHeartbeatsPool.scheduleAtFixedRate(task, 0, cluster.configuration.heartbeatsInterval, TimeUnit.MILLISECONDS)

  }

  def stop() = {
    LOG.debug("Stop Heartbeater")
    scheduledHeartbeatsPool.shutdownNow()
  }
}