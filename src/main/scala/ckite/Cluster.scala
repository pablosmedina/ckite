package ckite

import java.util.concurrent.{ ConcurrentHashMap, Executors, TimeoutException }
import java.util.concurrent.atomic.AtomicReference

import ckite.exception.LeaderTimeoutException
import ckite.rlog.Snapshot
import ckite.rpc.{ AppendEntries, AppendEntriesResponse, ClusterConfigurationCommand, Command, JointConfiguration, MajorityJointConsensus, NewConfiguration, ReadCommand, RequestVote, RequestVoteResponse }
import ckite.statemachine.StateMachine
import ckite.stats.{ ClusterStatus, LogStatus, Status }
import ckite.util.Logging
import com.twitter.concurrent.NamedPoolThreadFactory

import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ DurationInt, DurationLong }
import scala.concurrent.{ Await, Future, Promise }
import scala.util.{ Failure, Success, Try }
import scala.util.control.Breaks.{ break, breakable }

class Cluster(stateMachine: StateMachine, val configuration: Configuration) extends Logging {

  val local = new LocalMember(this, configuration.localBinding)
  val consensusMembership = new AtomicReference[Membership](EmptyMembership)
  val rlog = new RLog(this, stateMachine)

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

  def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    log.debug("RequestVote received: {}", requestVote)
    obtainRemoteMember(requestVote.memberId).map { remoteMember ⇒
      local on requestVote
    }.getOrElse {
      log.warn("Reject vote to member {} who is not present in the Cluster", requestVote.memberId)
      Future.successful(RequestVoteResponse(local term, false))
    }
  }

  def obtainRemoteMember(memberId: String): Option[RemoteMember] = (membership.remoteMembers).find {
    _.id == memberId
  }

  def membership = consensusMembership.get()

  def on(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    local on appendEntries
  }

  def broadcastAppendEntries(term: Int) = {
    if (term == local.term) {
      membership.remoteMembers foreach { member ⇒
        Future {
          member sendAppendEntries term
        }
      }
    }
  }

  def onLocal[T](readCommand: ReadCommand[T]) = rlog execute readCommand

  def collectVotes(term: Int): Seq[Member] = {
    if (!hasRemoteMembers) return Seq(local)
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

  def hasRemoteMembers = !membership.remoteMembers.isEmpty

  def forwardToLeader[T](command: Command): Future[T] = withLeader { leader ⇒
    log.debug("Forward command {}", command)
    leader.forwardCommand[T](command)
  }

  def withLeader[T](f: Member ⇒ Future[T]): Future[T] = {
    local.currentState.leaderPromise.future.flatMap(f)
  }

  def addMember(memberBinding: String) = {
    if (membership.allBindings contains memberBinding) {
      log.info("The member {} is already present in the cluster", memberBinding)
      Future.successful(true)
    } else {
      log.info("The member {} is going to be added to the cluster", memberBinding)

      val newMemberBindings = membership.allBindings :+ memberBinding
      changeClusterConfiguration(newMemberBindings)
    }
  }

  private def changeClusterConfiguration(members: Seq[String]) = {
    on[Boolean](JointConfiguration(consensusMembership.get().allBindings.toList, members.toList))
  }

  def on[T](command: Command): Future[T] = {
    havingLeader {
      log.debug("Command received: {}", command)
      local.on[T](command)
    }
  }

  def havingLeader[T](f: ⇒ Future[T]): Future[T] = {
    local.currentState.leaderPromise.future.flatMap { member ⇒
      f
    }
  }

  def removeMember(memberBinding: String) = {
    log.info("The member {} is going to be removed from the cluster", memberBinding)
    val newMemberBindings = membership.allBindings diff Seq(memberBinding)
    changeClusterConfiguration(newMemberBindings)
  }

  //EnterJointConsensus reached quorum. Send LeaveJointConsensus If I'm the Leader to notify the new membership.
  def on(majorityJointConsensus: MajorityJointConsensus) = {
    local on majorityJointConsensus
  }

  def apply(index: Long, clusterConfiguration: ClusterConfigurationCommand) = {
    if (membership.index < index) {
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

    SimpleConsensus(localOption, bindingsWithoutLocal.map { binding ⇒ obtainRemoteMember(binding).getOrElse(new RemoteMember(this, binding)) }, index)
  }

  def installSnapshot(snapshot: Snapshot): Boolean = {
    log.debug("InstallSnapshot received")
    rlog.snapshotManager.installSnapshot(snapshot)
  }

  def getMembers(): List[String] = membership.allBindings.toList

  def isActiveMember(memberId: String): Boolean = membership.allBindings.contains(memberId)

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

  def obtainMember(memberId: String): Option[Member] = (membership.allMembers).find {
    _.id == memberId
  }

}