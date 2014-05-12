package ckite

import ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import org.slf4j.MDC
import ckite.util.Logging
import ckite.rpc.WriteCommand
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.AppendEntries
import java.util.concurrent.Executors
import ckite.rpc.JointConfiguration
import ckite.rpc.NewConfiguration
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.RequestVoteResponse
import ckite.statemachine.StateMachine
import com.typesafe.config.Config
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import ckite.exception.LeaderTimeoutException
import scala.util.Success
import java.util.concurrent.TimeoutException
import java.util.concurrent.Callable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import ckite.util.CKiteConversions._
import ckite.rpc.ReadCommand
import ckite.rpc.Command
import org.mapdb.DBMaker
import java.io.File
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.Executors.DefaultThreadFactory
import com.twitter.concurrent.NamedPoolThreadFactory
import ckite.rpc.JointConfiguration
import ckite.rlog.Snapshot
import scala.util.control.Breaks._
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.util.Try
import scala.util.Success
import ckite.stats.ClusterStatus
import ckite.stats.LogStatus
import ckite.stats.Status
import ckite.rpc.ClusterConfigurationCommand
import ckite.rpc.NewConfiguration
import ckite.rpc.NewConfiguration

class Cluster(stateMachine: StateMachine, val configuration: Configuration) extends Logging {

  val local = new LocalMember(this, configuration.localBinding)
  val consensusMembership = new AtomicReference[Membership](EmptyMembership)
  val rlog = new RLog(this, stateMachine)

  val leaderPromise = new AtomicReference[Promise[Member]](Promise[Member]())

  val appendEntriesPool = new ThreadPoolExecutor(0, configuration.appendEntriesWorkers,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("AppendEntries-worker", true))
  val appendEntriesExecutionContext = ExecutionContext.fromExecutor(appendEntriesPool)

  val electionPool = new ThreadPoolExecutor(0, configuration.electionWorkers,
    15L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("Election-worker", true))
  val electionExecutionContext = ExecutionContext.fromExecutor(electionPool)

  val scheduledElectionTimeoutExecutor = Executors.newScheduledThreadPool(1, new NamedPoolThreadFactory("ElectionTimeout-worker", true))

  val waitForLeaderTimeout = configuration.waitForLeaderTimeout millis

  def start = {
    LOG.info("Starting CKite...")

    local becomeStarter

    if (configuration.bootstrap) {
      startBootstrap
    } else if (isEmptyConfiguration) {
      startJoining
    } else {
      startNormal
    }

  }

  private def startNormal = {
    //start as a normal follower
    LOG.info("Existing configuration. Start normal")
    local becomeFollower
  }

  private def startBootstrap = {
    LOG.info("Bootstrapping a new Cluster...")

    rlog.assertEmptyLog

    rlog.assertNoSnapshot

    //validate empty log and no snapshot
    consensusMembership.set(new SimpleConsensus(Some(local), Seq(), 0))
    local becomeFollower

    LOG.info("Will set a configuration with just myself: {}", local.id)
    on[Boolean](NewConfiguration(List(local.id))) //the initial configuration must be saved in the log
  }

  private def startJoining = {
    //no configuration. will try to join an existing cluster
    LOG.info("Empty log & no snapshot")
    LOG.info("Will try to Join an existing Cluster using the seeds: {}", configuration.memberBindings)

    local becomeFollower

    breakable {
      for (remoteMemberBinding <- configuration.memberBindings) {
        LOG.info("Try to join with {}", remoteMemberBinding)
        val remoteMember = new RemoteMember(this, remoteMemberBinding)
        val response = remoteMember.join(local.id)
        if (response.success) {
          LOG.info("Join was successful")
          break
        }
      }
    }
  }

  private def isEmptyConfiguration = consensusMembership.get() == EmptyMembership

  def stop = {
    LOG.info("Stopping CKite...")

    shutdownPools

    local stop

    rlog stop
  }

  private def shutdownPools = {
    appendEntriesPool.shutdown()
    electionPool.shutdown()
    scheduledElectionTimeoutExecutor.shutdown()
  }

  def on(requestVote: RequestVote): RequestVoteResponse = {
    LOG.debug("RequestVote received: {}", requestVote)
    obtainRemoteMember(requestVote.memberId).map { remoteMember =>
      local on requestVote
    }.getOrElse {
      LOG.warn("Reject vote to member {} who is not present in the Cluster", requestVote.memberId)
      RequestVoteResponse(local term, false)
    }
  }

  def on(appendEntries: AppendEntries) = {
    local on appendEntries
  }

  def on[T](command: Command): T = {
    havingLeader {
      LOG.debug("Command received: {}", command)
      local.on[T](command)
    }
  }

  def broadcastAppendEntries(term: Int)(implicit context: ExecutionContext = appendEntriesExecutionContext) = {
    if (term == local.term) {
      membership.remoteMembers foreach { member =>
        future {
          member sendAppendEntries term
        }
      }
    }
  }

  def onLocal(readCommand: ReadCommand) = rlog execute readCommand

  def collectVotes(implicit context: ExecutionContext = electionExecutionContext): Seq[Member] = {
    if (!hasRemoteMembers) return Seq(local)
    val promise = Promise[Seq[Member]]()
    val votes = new ConcurrentHashMap[Member, Boolean]()
    votes.put(local, true)
    membership.remoteMembers.foreach { remoteMember =>
      future {
        (remoteMember, remoteMember requestVote)
      } onSuccess {
        case (member, vote) =>
          votes.put(member, vote)
          val grantedVotes = votes.asScala.filter { tuple => tuple._2 }.keySet.toSeq
          val rejectedVotes = votes.asScala.filterNot { tuple => tuple._2 }.keySet.toSeq
          if (membership.reachMajority(grantedVotes) ||
            membership.reachAnyMajority(rejectedVotes) ||
            membership.allMembers.size == votes.size())
            promise.trySuccess(grantedVotes)
      }
    }
    Try {
      Await.result(promise.future, configuration.collectVotesTimeout millis)
    } getOrElse {
      votes.asScala.filter { tuple => tuple._2 }.keySet.toSeq
    }
  }

  def forwardToLeader[T](command: Command): T = withLeader { leader =>
    LOG.debug("Forward command {}", command)
    leader.forwardCommand[T](command)
  }

  def addMember(memberBinding: String) = {
    if (membership.allBindings contains memberBinding) {
      LOG.info("The member {} is already present in the cluster", memberBinding)
      true
    } else {
      LOG.info("The member {} is going to be added to the cluster", memberBinding)

      val newMemberBindings = membership.allBindings :+ memberBinding
      changeClusterConfiguration(newMemberBindings)
    }
  }

  def removeMember(memberBinding: String) = {
    LOG.info("The member {} is going to be removed from the cluster", memberBinding)
    val newMemberBindings = membership.allBindings diff Seq(memberBinding)
    changeClusterConfiguration(newMemberBindings)
  }

  private def changeClusterConfiguration(members: Seq[String]) = {
    on[Boolean](JointConfiguration(consensusMembership.get().allBindings.toList, members.toList))
  }

  //EnterJointConsensus reached quorum. Send LeaveJointConsensus If I'm the Leader to notify the new membership.
  def on(majorityJointConsensus: MajorityJointConsensus) = {
    local on majorityJointConsensus
  }

  def apply(index: Long, clusterConfiguration: ClusterConfigurationCommand) = {
    if (membership.index < index) {
      LOG.info(s"Applying {}", clusterConfiguration)
      clusterConfiguration match {
        case JointConfiguration(oldBindings, newBindings) => {
          //EnterJointConsensus received. Switch membership to JointConsensus
          setNewMembership(JointConsensus(simpleConsensus(oldBindings), simpleConsensus(newBindings), index))
        }
        case NewConfiguration(newBindings) => {
          //LeaveJointConsensus received. A new membership has been set. Switch to SimpleConsensus or shutdown If no longer part of the cluster.
          setNewMembership(simpleConsensus(newBindings, index))
        }
      }
    }
  }

  private def setNewMembership(membership: Membership) = {
    consensusMembership.set(membership)
    LOG.info("New cluster configuration is {}", consensusMembership.get())
  }

  private def simpleConsensus(bindings: Seq[String], index: Long = 0): SimpleConsensus = {
    val localOption = if (bindings.contains(local.id)) Some(local) else None
    val bindingsWithoutLocal = bindings diff Seq(local.id).toSet.toSeq

    SimpleConsensus(localOption, bindingsWithoutLocal.map { binding => obtainRemoteMember(binding).getOrElse(new RemoteMember(this, binding)) }, index)
  }

  def membership = consensusMembership.get()

  def hasRemoteMembers = !membership.remoteMembers.isEmpty

  def updateLeader(leaderId: String): Boolean = leaderPromise.synchronized {
    obtainMember(leaderId) map { newLeader =>
      val promise = leaderPromise.get()
      val isNew = !promise.isCompleted || promise.future.value.get.get != newLeader
      if (isNew) {
        leaderPromise.get().success(newLeader) //complete current promise for operations waiting for it
        leaderPromise.set(Promise.successful(newLeader)) //kept promise for subsequent leader 
      }
      isNew
    } getOrElse (false)
  }

  def setNoLeader = leaderPromise.synchronized {
    if (leaderPromise.get().isCompleted) {
      leaderPromise.set(Promise[Member]())
    }
  }

  def awaitLeader: Member = {
    try {
      Await.result(leaderPromise.get().future, waitForLeaderTimeout)
    } catch {
      case e: TimeoutException => {
        LOG.warn("Wait for Leader timed out after {}", waitForLeaderTimeout)
        throw new LeaderTimeoutException(e)
      }
    }
  }

  def leader: Option[Member] = {
    val promise = leaderPromise.get()
    if (promise.isCompleted) Some(promise.future.value.get.get) else None
  }

  def installSnapshot(snapshot: Snapshot): Boolean = {
    LOG.debug("InstallSnapshot received")
    rlog.snapshotManager.installSnapshot(snapshot)
  }

  def getMembers(): Seq[String] = membership.allBindings

  def isActiveMember(memberId: String): Boolean = membership.allBindings.contains(memberId)

  def obtainRemoteMember(memberId: String): Option[RemoteMember] = (membership.remoteMembers).find { _.id == memberId }

  def anyLeader = leader != None

  def majority = membership.majority

  def reachMajority(members: Seq[Member]): Boolean = membership.reachMajority(members)

  def getStatus = {
    val clusterStatus = ClusterStatus(local.term, local.currentState.toString(), local.currentState.info())
    val logStatus = LogStatus(rlog.size, rlog.commitIndex.intValue(), rlog.getLastLogEntry)
    Status(clusterStatus, logStatus)
  }

  def withLeader[T](f: Member => T): T = {
    val leader = awaitLeader
    f(leader)
  }

  def havingLeader[T](f: => T): T = {
    awaitLeader
    f
  }

  private def obtainMember(memberId: String): Option[Member] = (membership.allMembers).find { _.id == memberId }

}