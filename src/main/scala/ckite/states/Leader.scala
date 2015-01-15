package ckite.states

import java.lang.Boolean
import java.util.concurrent.{ ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit }

import ckite.{ Cluster, Member, RemoteMember }
import ckite.rpc._
import ckite.stats.{ FollowerInfo, LeaderInfo, StateInfo }
import ckite.util.CKiteConversions.fromFunctionToRunnable
import ckite.util.{ CustomThreadFactory, Logging }

import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.DurationLong

case class Leader(cluster: Cluster, term: Int, leaderPromise: Promise[Member]) extends State(Some(cluster.local.id)) {

  val ReplicationTimeout = cluster.configuration.appendEntriesTimeout
  val startTime = System.currentTimeMillis()

  val heartbeater = new Heartbeater(cluster)
  val followersInfo = new ConcurrentHashMap[String, Long]()

  val appendEntriesTimeout = cluster.configuration.appendEntriesTimeout millis

  override def begin() = {
    if (term < cluster.local.term) {
      log.debug(s"Cant be a Leader of term $term. Current term is ${cluster.local.term}")
      cluster.local.becomeFollower(cluster.local.term)
    } else {
      resetLastLog()
      resetNextAndMatchIndexes()
      resetFollowerInfo()
      heartbeater start term
      appendNoOp
      log.info(s"Start being $this")
      leaderPromise.success(cluster.local)
      cluster.local.persistState
    }
  }

  private def appendNoOp = {
    if (cluster.rlog.lastLog.longValue() == 0) {
      log.info("Will set a configuration with just myself: {}", cluster.local.id)
      on[Boolean](NewConfiguration(List(cluster.local.id))) //the initial configuration must be saved in the log
    } else {
      log.debug("Append a NoOp as part of Leader initialization")
      on[Unit](NoOp())
    }
  }

  private def resetLastLog() = cluster.rlog.resetLastLog()

  private def resetNextAndMatchIndexes() = {
    val nextIndex = cluster.rlog.lastLog.longValue() + 1
    cluster.membership.remoteMembers.foreach { member ⇒ member.setNextLogIndex(nextIndex); member.resetMatchIndex }
  }

  override def stop(stopTerm: Int) = {
    if (stopTerm > term) {
      heartbeater stop

      log.debug("Stop being Leader")
    }
  }

  override def on[T](command: Command): Future[T] = {
    command match {
      case w: WriteCommand[T] ⇒ onWriteCommand[T](w)
      case r: ReadCommand[T]  ⇒ onReadCommand[T](r)
    }
  }

  private def onWriteCommand[T](write: WriteCommand[T]): Future[T] = {
    cluster.rlog.append[T](write) flatMap { tuple ⇒
      val logEntry = tuple._1
      val valuePromise = tuple._2
      replicate(logEntry)
      valuePromise.future
    }
  }

  private def replicate(logEntry: LogEntry) = {
    if (cluster.membership.hasRemoteMembers()) {
      cluster.broadcastAppendEntries(logEntry.term)
    } else {
      log.debug("No member to replicate")
      cluster.rlog commit logEntry.index
    }
  }

  private def onReadCommand[T](command: ReadCommand[T]): Future[T] = Future {
    cluster.rlog execute command
  }

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    if (appendEntries.term < term) {
      rejectAppendEntries(appendEntries, "old term")
    } else {
      stepDown(appendEntries.term, Some(appendEntries.leaderId))
      cluster.local.onAppendEntries(appendEntries)
    }
  }

  override def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    if (requestVote.term <= term) {
      rejectVote(requestVote.memberId, s"being Leader in term $term")
    } else {
      stepDown(requestVote.term, None)
      cluster.local onRequestVote requestVote
    }
  }

  override def on(jointConsensusCommited: MajorityJointConsensus) = {
    log.debug(s"JointConfiguration is committed... will switch to NewConfiguration")
    cluster.onCommandReceived[Boolean](NewConfiguration(jointConsensusCommited.newBindings))
  }

  override protected def getCluster: Cluster = cluster

  override def toString = s"Leader[$term]"

  override def info(): StateInfo = {
    val now = System.currentTimeMillis()
    val followers = followersInfo.map {
      tuple ⇒
        val member = cluster.membership.obtainRemoteMember(tuple._1).get
        (tuple._1, FollowerInfo(lastAck(tuple._2, now), member.matchIndex.intValue(), member.nextLogIndex.intValue()))
    }
    LeaderInfo(leaderUptime.toString, followers.toMap)
  }

  private def leaderUptime = System.currentTimeMillis() - startTime millis

  private def lastAck(ackTime: Long, now: Long) = if (ackTime > 0) (now - ackTime millis).toString else "Never"

  private def resetFollowerInfo() = {
    cluster.membership.remoteMembers.foreach { member ⇒ followersInfo.put(member.id, -1) }
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
        log.debug(s"Next LogIndex #$nextIndex to be sent to ${member} is contained in a Snapshot. An InstallSnapshot will be sent.")
        sendInstallSnapshot(member)
      }
    }
    followersInfo.put(member.id, time)
  }

  private def onAppendEntriesResponseUpdateNextLogIndex(member: RemoteMember, appendEntries: AppendEntries, appendEntriesResponse: AppendEntriesResponse) = {
    val lastIndexSent = appendEntries.entries.last.index
    if (appendEntriesResponse.success) {
      member.ackLogEntry(lastIndexSent)
      log.debug(s"Member ${member} ack - index sent #$lastIndexSent - next index #${member.nextLogIndex}")
      tryToCommitEntries(lastIndexSent)
    } else {
      member.decrementNextLogIndex()
      log.debug(s"Member ${member} reject - index sent #$lastIndexSent - next index is #${member.nextLogIndex}")
    }
  }

  private def tryToCommitEntries(lastEntrySent: Long) = {
    val currentCommitIndex = cluster.rlog.commitIndex.longValue()
    (currentCommitIndex + 1) to lastEntrySent foreach { index ⇒
      if (reachMajority(index)) {
        cluster.rlog commit index
      }
    }
  }

  private def reachMajority(index: Long) = cluster.membership.reachMajority(membersHavingAtLeast(index) :+ cluster.local)

  private def membersHavingAtLeast(index: Long): Seq[RemoteMember] = {
    cluster.membership.remoteMembers.filter { remoteMember ⇒ remoteMember.matchIndex.longValue() >= index }
  }

  private def isLogEntryInSnapshot(logIndex: Int): Boolean = {
    cluster.rlog.snapshotManager.isInSnapshot(logIndex)
  }

  def sendInstallSnapshot(member: RemoteMember) = {
    cluster.rlog.snapshotManager.latestSnapshot map { snapshot ⇒
      val installSnapshot = InstallSnapshot(snapshot)
      log.debug(s"Sending $installSnapshot to ${member}")
      member.sendInstallSnapshot(installSnapshot).map { response ⇒
        if (response.success) {
          log.debug("Successful InstallSnapshot")
          member.ackLogEntry(snapshot.lastLogEntryIndex)
          tryToCommitEntries(snapshot.lastLogEntryIndex)
        } else {
          log.debug("Failed InstallSnapshot")
        }
        member.enableReplications()
      }

    }
  }
}

class Heartbeater(cluster: Cluster) extends Logging {

  val scheduledHeartbeatsPool = new ScheduledThreadPoolExecutor(1, CustomThreadFactory("Heartbeater", true))

  def start(term: Int) = {
    log.debug("Start Heartbeater")

    val task: Runnable = () ⇒ {
      log.trace(s"Leader[$term] broadcasting hearbeats")
      cluster.broadcastAppendEntries(term)
    }
    scheduledHeartbeatsPool.scheduleAtFixedRate(task, 0, cluster.configuration.heartbeatsInterval, TimeUnit.MILLISECONDS)

  }

  def stop() = {
    log.debug("Stop Heartbeater")
    scheduledHeartbeatsPool.shutdownNow()
  }
}