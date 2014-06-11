package ckite.states

import java.lang.Boolean
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationLong
import scala.concurrent.future

import com.twitter.concurrent.NamedPoolThreadFactory

import ckite.Cluster
import ckite.Member
import ckite.RemoteMember
import ckite.rpc.AppendEntries
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.Command
import ckite.rpc.LogEntry
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.NewConfiguration
import ckite.rpc.NoOp
import ckite.rpc.ReadCommand
import ckite.rpc.RequestVote
import ckite.rpc.RequestVoteResponse
import ckite.rpc.WriteCommand
import ckite.stats.FollowerInfo
import ckite.stats.LeaderInfo
import ckite.stats.StateInfo
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.Logging

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
class Leader(cluster: Cluster, term: Int, leaderPromise: Promise[Member]) extends State(term, leaderPromise, Some(cluster.local.id)) {

  val ReplicationTimeout = cluster.configuration.appendEntriesTimeout
  val startTime = System.currentTimeMillis()
  
  val heartbeater = new Heartbeater(cluster)
  val followersInfo = new ConcurrentHashMap[String, Long]()
  
  val appendEntriesTimeout = cluster.configuration.appendEntriesTimeout millis
  
  override def begin() = {
    if (term < cluster.local.term) {
      LOG.debug(s"Cant be a Leader of term $term. Current term is ${cluster.local.term}")
      cluster.local.becomeFollower(cluster.local.term)
    } else {
      resetLastLog
      resetNextAndMatchIndexes
      resetFollowerInfo
      heartbeater start term
      appendNoOp
      LOG.info(s"Start being $this")
      leaderPromise.success(cluster.local)
      cluster.local.persistState
    }
  }
  
  private def appendNoOp = {
	  if (cluster.rlog.lastLog.longValue() == 0) {
	    LOG.info("Will set a configuration with just myself: {}", cluster.local.id)
	    on[Boolean](NewConfiguration(List(cluster.local.id))) //the initial configuration must be saved in the log
	  } else {
		  LOG.debug("Append a NoOp as part of Leader initialization")
		  on[Unit](NoOp())
	  }
  }
  
  private def resetLastLog = cluster.rlog.resetLastLog()

  private def resetNextAndMatchIndexes = {
    val nextIndex = cluster.rlog.lastLog.longValue() + 1
    cluster.membership.remoteMembers.foreach { member => member.setNextLogIndex(nextIndex); member.resetMatchIndex }
  }

  override def stop(stopTerm: Int) = {
    if (stopTerm > term) {
    	heartbeater stop
    	
    	LOG.debug("Stop being Leader")
    } 
  }

  override def on[T](command: Command): Future[T] = {
    command match {
      case w: WriteCommand[T] => onWriteCommand[T](w)
      case r: ReadCommand[T] => onReadCommand[T](r)
    }
  }

  private def onWriteCommand[T](write: WriteCommand[T]): Future[T] = {
    cluster.rlog.append[T](write) flatMap { tuple =>
      val logEntry = tuple._1
      val valuePromise = tuple._2
      replicate(logEntry)
      valuePromise.future
    }
  }
  
  private def replicate(logEntry: LogEntry) = {
    if (cluster.hasRemoteMembers) {
      cluster.broadcastAppendEntries(logEntry.term)
    } else {
      LOG.debug("No member to replicate")
      cluster.rlog commit logEntry.index
    }
  }
  
  
  private def onReadCommand[T](command: ReadCommand[T]): Future[T] = future {
    	cluster.rlog execute command
  }

  override def on(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    if (appendEntries.term < term) {
      Future.successful(AppendEntriesResponse(term, false))
    } else {
      stepDown(appendEntries.term, Some(appendEntries.leaderId))
      cluster.local on appendEntries
    }
  }

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    if (requestVote.term <= term) {
      Future.successful(RequestVoteResponse(term, false))
    } else {
      stepDown(requestVote.term, None)
      cluster.local on requestVote
    }
  }

  override def on(jointConsensusCommited: MajorityJointConsensus) = {
    LOG.debug(s"JointConfiguration is committed... will switch to NewConfiguration")
    cluster.on[Boolean](NewConfiguration(jointConsensusCommited.newBindings))
  }
  
  override protected def getCluster: Cluster = cluster

  override def toString = s"Leader[$term]"

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
    val lastIndexSent = appendEntries.entries.last.index
    if (appendEntriesResponse.success) {
      member.ackLogEntry(lastIndexSent)
      LOG.debug(s"Member ${member} ack - index sent #$lastIndexSent - next index #${member.nextLogIndex}")
      tryToCommitEntries(lastIndexSent)
    } else {
      member.decrementNextLogIndex()
      LOG.debug(s"Member ${member} reject - index sent #$lastIndexSent - next index is #${member.nextLogIndex}")
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


class Heartbeater(cluster: Cluster) extends Logging {

  val scheduledHeartbeatsPool = new ScheduledThreadPoolExecutor(1, new NamedPoolThreadFactory("Heartbeater", true))
  
  def start(term: Int) = {
    LOG.debug("Start Heartbeater")

    val task:Runnable = () => {
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