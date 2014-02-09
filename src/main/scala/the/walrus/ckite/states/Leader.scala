package the.walrus.ckite.states

import java.util.concurrent.Executors
import java.util.concurrent.Future
import scala.util.Try
import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.WriteCommand
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import the.walrus.ckite.executions.Executions
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.rpc.AppendEntries
import the.walrus.ckite.rpc.LogEntry
import the.walrus.ckite.RLog
import the.walrus.ckite.executions.Executions
import the.walrus.ckite.executions.ExpectedResultFilter
import java.lang.Boolean
import the.walrus.ckite.Member
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.LeaveJointConsensus
import the.walrus.ckite.exception.NoMajorityReachedException
import the.walrus.ckite.util.CKiteConversions._
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command
import java.util.concurrent.ScheduledFuture
import com.twitter.util.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.SynchronousQueue
import com.twitter.concurrent.NamedPoolThreadFactory
import the.walrus.ckite.RemoteMember
import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.collection.mutable.ArrayBuffer
import the.walrus.ckite.rpc.NoOp
import the.walrus.ckite.rpc.NoOp

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

  val heartbeater = new Heartbeater(cluster)
  val replicator = new Replicator(cluster)
  val startTime = System.currentTimeMillis()
  val followersInfo = new ConcurrentHashMap[String, Long]()

  override def begin(term: Int) = {
    if (term < cluster.local.term) {
      LOG.debug(s"Cant be a Leader of term $term. Current term is ${cluster.local.term}")
      cluster.local.becomeFollower(cluster.local.term)
    } else {
      cluster.updateLeader(cluster.local.id)
      resetLastLog
      resetNextIndexes
      resetFollowerInfo
      heartbeater start term
      async {
    	  on[Unit](NoOp())
      }
      LOG.info(s"Start being Leader")
    }
  }
  
  private def async(block: => Unit) = {
    cluster.replicatorExecutor.execute { cluster.inContext {
    		LOG.info("Replicate a NoOp as part of Leader initialization")
    		block
    	}
      }
  }
  
  private def noOp(): LogEntry = LogEntry(cluster.local.term, cluster.rlog.nextLogIndex, NoOp())

  private def resetLastLog = cluster.rlog.resetLastLog()

  private def resetNextIndexes = {
    val nextIndex = cluster.rlog.lastLog.intValue() + 1
    cluster.membership.remoteMembers.foreach { member => member.setNextLogIndex(nextIndex) }
  }

  override def stop = {
    LOG.info("Stop being Leader")
    heartbeater stop

    cluster.setNoLeader
  }

  override def on[T](command: Command): T = {
    command match {
      case w: WriteCommand => onWriteCommand[T](w)
      case r: ReadCommand => onReadCommand[T](r)
    }

  }

  private def onWriteCommand[T](command: WriteCommand): T = cluster.local.locked {
    val logEntry = LogEntry(cluster.local.term, cluster.rlog.nextLogIndex, command)
    cluster.rlog.append(List(logEntry))
    LOG.info(s"Replicating log entry $logEntry")
    val replicationAcks = replicate(logEntry)
    if (cluster.reachMajority(replicationAcks :+ cluster.local)) {
      (cluster.rlog commit logEntry).asInstanceOf[T]
    } else {
      LOG.warn(s"ReplicationTimeout! Could not commit $logEntry due to no majority")
      throw new NoMajorityReachedException(logEntry)
    }
  }
  
  private def replicate(logEntry: LogEntry): Seq[Member] = {
    if (cluster.hasRemoteMembers) {
      val acks = replicator.replicate(logEntry)
      LOG.info(s"Got replication acks from $acks")
      acks
    } else {
      LOG.info(s"No member to replicate")
      Seq()
    }
  }

  private def onReadCommand[T](command: ReadCommand): T = {
    (cluster.rlog execute command).asInstanceOf[T]
  }

  override def on(appendEntries: AppendEntries): AppendEntriesResponse = {
    if (appendEntries.term < cluster.local.term) {
      AppendEntriesResponse(cluster.local.term, false)
    } else {
      stepDown(Some(appendEntries.leaderId), appendEntries.term)
      cluster.local on appendEntries
    }
  }

  override def on(requestVote: RequestVote): RequestVoteResponse = {
    if (requestVote.term <= cluster.local.term) {
      RequestVoteResponse(cluster.local.term, false)
    } else {
      stepDown(None, requestVote.term)
      cluster.local on requestVote
    }
  }

  override def on(jointConsensusCommited: MajorityJointConsensus) = {
    LOG.info(s"Sending LeaveJointConsensus")
    cluster.on[Boolean](LeaveJointConsensus(jointConsensusCommited.newBindings))
  }
  
  override protected def getCluster: Cluster = cluster

  override def toString = "Leader"
    
 override def info(): StateInfo = {
    val now = System.currentTimeMillis()
    val unit = TimeUnit.MILLISECONDS
    val f = followersInfo.map {
      tuple => (tuple._1, FollowerInfo(if (tuple._2 > 0) Duration(now - tuple._2, unit).toString else "Never", 
          cluster.obtainRemoteMember(tuple._1).get.nextLogIndex.intValue()))  }
    LeaderInfo(Duration(now - startTime, unit).toString, f.toMap)
  }
  
  private def resetFollowerInfo = {
    cluster.membership.remoteMembers.foreach { member => followersInfo.put(member.id, -1) }
  }
  
  override def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {
      val time = System.currentTimeMillis() 
	  if (!request.entries.isEmpty) {
            onAppendEntriesResponseUpdateNextLogIndex(member, request, response)
       }
      val nextIndex = member.nextLogIndex.intValue()
      val nextIndexPrevious = nextIndex - 1
       if (isLogEntryInSnapshot(nextIndex) || isLogEntryInSnapshot(nextIndexPrevious)) {
          val wasEnabled = member.disableReplications()
          if (wasEnabled) { 
        	LOG.info(s"Next LogIndex #$nextIndex (or its previous #$nextIndexPrevious) to be sent to ${member} is contained in a Snapshot. An InstallSnapshot will be sent.")
            sendSnapshotAsync(member)
          }
      }
      followersInfo.put(member.id, time)
  }

  private def onAppendEntriesResponseUpdateNextLogIndex(member: RemoteMember, appendEntries: AppendEntries, appendEntriesResponse: AppendEntriesResponse) = {
      if (appendEntriesResponse.success) {
        member.nextLogIndex.incrementAndGet()
      } else {
        val currentIndex = member.nextLogIndex.decrementAndGet()
        if (currentIndex == 0) member.nextLogIndex.set(1)
      }
      appendEntries.entries.last.index
      LOG.debug(s"Member ${member} $appendEntriesResponse - LogIndex sent #${appendEntries.entries.last.index} - next LogIndex is #${member.nextLogIndex}")
  }
  
  private def isLogEntryInSnapshot(logIndex: Int): Boolean = {
    val some = cluster.rlog.getSnapshot().map {snapshot => logIndex <= snapshot.lastLogEntryIndex }
    some.getOrElse(false).asInstanceOf[Boolean]
  }
  
  def sendSnapshotAsync(member: RemoteMember) = {
	 val snapshot = cluster.rlog.getSnapshot().get
     LOG.info(s"Sending InstallSnapshot to ${member} containing $snapshot")
     com.twitter.util.Future {
		 member.sendSnapshot(snapshot)
	 }
  }
}

class Replicator(cluster: Cluster) extends Logging {

  val ReplicationTimeout = cluster.configuration.replicationTimeout
 
  //replicate and wait for a majority of acks. 
  def replicate(logEntry: LogEntry): Seq[Member] = {
    val execution = Executions.newExecution().withExecutor(cluster.replicatorExecutor)
    cluster.membership.remoteMembers.foreach { member =>
      execution.withTask {
        (member, member replicate logEntry)
      }
    }
    LOG.debug(s"Waiting for a majority consisting of ${cluster.membership.majoritiesMap} until $ReplicationTimeout ms")
    val rawResults = execution.withTimeout(ReplicationTimeout, TimeUnit.MILLISECONDS)
      .withExpectedResults(1, new ReachMajorities(cluster)).execute[(Member, Boolean)]()
    val results: Iterable[(Member, Boolean)] = rawResults
    val mapres = results.filter { result => result._2 }.map { result => result._1 }
    mapres.toSeq
  }
  
}

class ReachMajorities(cluster: Cluster) extends ExpectedResultFilter with Logging {
  
  val members = ArrayBuffer[Member]()
  members.add(cluster.local)
  
  override def matches(x: Any) = {
	  val memberAck = x.asInstanceOf[(Member, Boolean)]
	  val ack = memberAck._2
	  val member = memberAck._1
	  if (ack) {
		  members.add(member)
	  }
	  if (cluster.membership.reachMajority(members.toSeq)) 1 else 0
  }
}

class Heartbeater(cluster: Cluster) extends Logging {

  val scheduledHeartbeatsPool = new ScheduledThreadPoolExecutor(1, new NamedPoolThreadFactory("Heartbeater", true))

  def start(term: Int) = {
    LOG.debug("Start Heartbeater")

    val task:Runnable = {
      cluster updateContextInfo

      LOG.trace("Heartbeater running")
      cluster.broadcastHeartbeats(term)
    } 
    scheduledHeartbeatsPool.scheduleAtFixedRate(task, 0, cluster.configuration.heartbeatsInterval, TimeUnit.MILLISECONDS)

  }

  def stop() = {
    LOG.debug("Stop Heartbeater")
    scheduledHeartbeatsPool.shutdownNow()
  }
}