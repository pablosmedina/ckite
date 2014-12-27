package ckite

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ ConcurrentHashMap, Executors, TimeoutException }

import ckite.exception.LeaderTimeoutException
import ckite.rlog.PersistentLog
import ckite.rpc._
import ckite.statemachine.StateMachine
import ckite.stats.{ ClusterStatus, LogStatus, Status }
import ckite.util.Logging
import com.twitter.concurrent.NamedPoolThreadFactory

import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ DurationInt, DurationLong }
import scala.concurrent.{ Await, Future, Promise }
import scala.util.control.Breaks.{ break, breakable }
import scala.util.{ Failure, Success, Try }

class Cluster(stateMachine: StateMachine, val configuration: Configuration, persistentLog: PersistentLog) extends Logging {

  val local = new LocalMember(this, configuration.localBinding)
  val consensusMembership = new AtomicReference[Membership](EmptyMembership)
  val rlog = new RLog(this, stateMachine, persistentLog)

  val scheduledElectionTimeoutExecutor = Executors.newScheduledThreadPool(1, new NamedPoolThreadFactory("ElectionTimeout-worker", true))

  val waitForLeaderTimeout = configuration.waitForLeaderTimeout millis

  def start = {
    log.info("Starting CKite...")

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
    log.info("Existing configuration. Start normal")
    val votedFor = local.votedFor.get()
    local.becomeFollower(term = local.persistedTerm.intValue(), vote = if (votedFor.isEmpty()) None else Some(votedFor))
  }

  private def startBootstrap = {
    log.info("Bootstrapping a new Cluster...")

    rlog.assertEmptyLog

    rlog.assertNoSnapshot

    //validate empty log and no snapshot
    consensusMembership.set(new SimpleConsensus(Some(local), Seq(), 0))

    local becomeFollower 0

    awaitLeader
  }

  private def startJoining = {
    //no configuration. will try to join an existing cluster
    log.info("Empty log & no snapshot")
    log.info("Will try to join an existing Cluster using the seeds: {}", configuration.memberBindings)

    local becomePassiveFollower 0 //don't start elections

    breakable {
      for (remoteMemberBinding ← configuration.memberBindings) {
        log.info("Try to join with {}", remoteMemberBinding)
        val remoteMember = new RemoteMember(this, remoteMemberBinding)
        val response = Await.result(remoteMember.join(local.id), 3 seconds) //TODO: Refactor me
        if (response.success) {
          log.info("Join successful")

          local.becomeFollower(term = 1, leaderPromise = local.currentState.leaderPromise)

          break
        }
      }
      //TODO: Implement retries/shutdown here
    }
  }

  private def isEmptyConfiguration = consensusMembership.get() == EmptyMembership

  def stop = {
    log.info("Stopping CKite...")

    shutdownPools

    local stop

    rlog stop
  }

  private def shutdownPools = {
    scheduledElectionTimeoutExecutor.shutdown()
  }

  def onRequestVoteReceived(requestVote: RequestVote): Future[RequestVoteResponse] = {
    log.debug("RequestVote received: {}", requestVote)
    local.onRequestVote(requestVote)
  }

  def onAppendEntriesReceived(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    log.trace(s"Received $appendEntries")
    local.onAppendEntries(appendEntries)
  }

  def onCommandReceived[T](command: Command): Future[T] = {
    log.debug("Command received: {}", command)
    local.on[T](command)
  }

  def onInstallSnapshotReceived(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = {
    log.debug("InstallSnapshot received")
    rlog.snapshotManager.installSnapshot(installSnapshot.snapshot).map(InstallSnapshotResponse(_))
  }

  def onLocalReadReceived[T](readCommand: ReadCommand[T]) = {
    rlog execute readCommand
  }

  def onMemberJoinReceived(memberBinding: String): Future[JoinMemberResponse] = {
    log.info(s"Join member request received. Member $memberBinding")
    if (membership.allBindings contains memberBinding) {
      log.info("The member {} is already present in the cluster", memberBinding)
      Future.successful(JoinMemberResponse(true))
    } else {
      log.info("The member {} is going to be added to the cluster", memberBinding)

      val newMemberBindings = membership.allBindings :+ memberBinding
      changeClusterConfiguration(newMemberBindings).map(JoinMemberResponse(_))
    }
  }

  def onMemberLeaveReceived(memberBinding: String) = {
    log.info("The member {} is going to be removed from the cluster", memberBinding)
    val newMemberBindings = membership.allBindings diff Seq(memberBinding)
    changeClusterConfiguration(newMemberBindings)
  }

  def membership = consensusMembership.get()

  def broadcastAppendEntries(term: Int) = {
    if (term == local.term) {
      membership.remoteMembers foreach { member ⇒
        Future {
          member sendAppendEntries term
        }
      }
    }
  }

  def collectVotes(term: Int): Seq[Member] = {
    if (!membership.hasRemoteMembers()) return Seq(local)
    val promise = Promise[Seq[Member]]()
    val votes = new ConcurrentHashMap[Member, Boolean]()
    votes.put(local, true)
    membership.remoteMembers.foreach { remoteMember ⇒
      Future {
        (remoteMember, remoteMember requestVote term)
      } onComplete {
        case Success((member, vote)) ⇒ {
          vote map { granted ⇒
            votes.put(member, granted)
            val grantedVotes = votes.asScala.filter { tuple ⇒ tuple._2 }.keySet.toSeq
            val rejectedVotes = votes.asScala.filterNot { tuple ⇒ tuple._2 }.keySet.toSeq
            if (membership.reachMajority(grantedVotes) ||
              membership.reachAnyMajority(rejectedVotes) ||
              membership.allMembers.size == votes.size())
              promise.trySuccess(grantedVotes)
          }
        }
        case Failure(reason) ⇒ {
          log.error("failure collecting votes", reason)
        }
      }
    }
    Try {
      Await.result(promise.future, configuration.collectVotesTimeout millis) //TODO: Refactor me
    } getOrElse {
      votes.asScala.filter { tuple ⇒ tuple._2 }.keySet.toSeq
    }
  }

  def forwardToLeader[T](command: Command): Future[T] = withLeader { leader ⇒
    log.debug("Forward command {}", command)
    leader.forwardCommand[T](command)
  }

  private def withLeader[T](f: Member ⇒ Future[T]): Future[T] = {
    local.currentState.leaderPromise.future.flatMap(f)
  }

  private def changeClusterConfiguration(members: Seq[String]): Future[Boolean] = {
    onCommandReceived[Boolean](JointConfiguration(consensusMembership.get().allBindings.toList, members.toList))
  }

  //EnterJointConsensus reached quorum. Send LeaveJointConsensus If I'm the Leader to notify the new membership.
  def onMajorityJointConsensusReceived(majorityJointConsensus: MajorityJointConsensus) = {
    local onMajorityJointConsensus majorityJointConsensus
  }

  def apply(index: Long, clusterConfiguration: ClusterConfigurationCommand) = {
    if (index > membership.index) {
      log.debug(s"Applying {}", clusterConfiguration)
      clusterConfiguration match {
        case JointConfiguration(oldBindings, newBindings) ⇒ {
          //EnterJointConsensus received. Switch membership to JointConsensus
          setNewMembership(JointConsensus(simpleConsensus(oldBindings), simpleConsensus(newBindings), index))
        }
        case NewConfiguration(newBindings) ⇒ {
          //LeaveJointConsensus received. A new membership has been set. Switch to SimpleConsensus or shutdown If no longer part of the cluster.
          setNewMembership(simpleConsensus(newBindings, index))
        }
      }
    }
  }

  private def setNewMembership(membership: Membership) = {
    consensusMembership.set(membership)
    log.info("Cluster Configuration changed to {}", consensusMembership.get())
  }

  private def simpleConsensus(bindings: Seq[String], index: Long = 0): SimpleConsensus = {
    val localOption = if (bindings.contains(local.id)) Some(local) else None
    val bindingsWithoutLocal = bindings diff Seq(local.id).toSet.toSeq

    SimpleConsensus(localOption, bindingsWithoutLocal.map { binding ⇒ membership.obtainRemoteMember(binding).getOrElse(new RemoteMember(this, binding)) }, index)
  }

  def anyLeader = leader != None

  def leader: Option[Member] = {
    val promise = local.currentState.leaderPromise
    if (promise.isCompleted) Some(promise.future.value.get.get) else None
  }

  def majority = membership.majority

  def reachMajority(members: Seq[Member]): Boolean = membership.reachMajority(members)

  def getStatus = {
    val clusterStatus = ClusterStatus(local.term, local.currentState.toString(), local.currentState.info())
    val logStatus = LogStatus(rlog.size, rlog.commitIndex.intValue(), rlog.getLastLogEntry)
    Status(clusterStatus, logStatus)
  }

  def isLeader = Try {
    awaitLeader == local
  }.getOrElse(false)

  private def awaitLeader: Member = {
    try {
      Await.result(local.currentState.leaderPromise.future, waitForLeaderTimeout)
    } catch {
      case e: TimeoutException ⇒ {
        log.warn("Wait for Leader in {} timed out after {}", waitForLeaderTimeout)
        throw new LeaderTimeoutException(e)
      }
    }
  }

}