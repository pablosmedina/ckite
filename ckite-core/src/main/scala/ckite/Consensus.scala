package ckite

import java.util.concurrent.atomic.AtomicReference

import ckite.rlog.{ Storage, Vote }
import ckite.rpc.LogEntry._
import ckite.rpc._
import ckite.states._
import ckite.stats.ConsensusStats
import ckite.util.Logging

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.Breaks._
import scala.concurrent.ExecutionContext.Implicits.global

case class Consensus(raft: Raft, storage: Storage, configuration: Configuration) extends Logging {

  private val currentState = new AtomicReference[State](Starter)
  private val ZERO_TERM = 0

  def membership = raft.membership

  private def log = raft.log

  private def state = currentState.get()

  def startAsBootstrapper() = {
    becomeFollower(ZERO_TERM)
  }

  def startAsFollower() = {
    storage.retrieveLatestVote() match {
      case Some(Vote(term, member)) ⇒ becomeFollower(term = term, vote = Option(member))
      case None                     ⇒ becomeFollower(ZERO_TERM)
    }
  }

  def startAsJoiner() = {
    //no configuration. will try to join an existing cluster
    logger.info("Start as Joiner. Using seeds: {}", configuration.memberBindings)

    becomeJoiner(ZERO_TERM) //don't start elections

    breakable {
      for (remoteMemberBinding ← configuration.memberBindings) {
        logger.info("Try to join with {}", remoteMemberBinding)
        val remoteMember = membership.get(remoteMemberBinding).get
        val response = Await.result(remoteMember.join(membership.myId), 3 seconds) //TODO: Refactor me
        if (response.success) {
          logger.info("Join successful")

          //becomeFollower(ZERO_TERM)

          break
        }
      }
      //TODO: Implement retries/shutdown here
    }
  }

  def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = state.onAppendEntries(appendEntries)

  def onRequestVote(requestVote: RequestVote): Future[RequestVoteResponse] = state.onRequestVote(requestVote)

  def onJointConfigurationCommitted(index: Index, jointConfiguration: JointConfiguration) = {
    if (membership.isCurrent(index)) { //TODO: move to Leader
      state.onJointConfigurationCommitted(jointConfiguration)
    }
    true
  }

  def onNewConfigurationCommitted(index: Index, configuration: NewConfiguration): Boolean = {
    true
  }

  def onCommand[T](command: Command): Future[T] = state.onCommand[T](command)

  def onInstallSnapshot(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = {
    state.onInstallSnapshot(installSnapshot)
  }

  def onMemberJoin(member: String): Future[JoinMemberResponse] = {
    if (!membership.contains(member)) {
      onCommand[Boolean](JointConfiguration(membership.members, membership.members + member)).map(JoinMemberResponse(_))
    } else {
      logger.info(s"$member is already part of the cluster")
      Future.successful(JoinMemberResponse(true))
    }
  }

  def onMemberLeave(member: String): Future[Boolean] = {
    if (membership.contains(member)) {
      onCommand(JointConfiguration(membership.members, membership.members - member))
    } else {
      Future.successful(true)
    }
  }

  def becomeLeader(term: Term) = {
    become(Leader(this, membership, log, term, leaderAnnouncer))
  }

  def becomeCandidate(term: Term) = {
    become(Candidate(this, membership, log, term, leaderAnnouncer.onElection))
  }

  def becomeFollower(term: Term, leaderAnnouncer: LeaderAnnouncer = LeaderAnnouncer(membership, configuration), vote: Option[String] = None) = {
    become(Follower(this, membership, log, term, leaderAnnouncer, vote))
  }

  def becomeJoiner(term: Term): Unit = {
    become(Joiner(this, membership, log, term, configuration))
  }

  def isLeader = {
    state.isLeader
  }

  def becomeStarter = changeState(Starter, Starter)

  def leaderAnnouncer = state.leaderAnnouncer

  private def become(newState: State) = {
    logger.trace("Trying to become {}", newState)
    var current = state
    //stops when current == newState or current.term < newState.term
    while (current.canTransitionTo(newState)) {
      if (changeState(current, newState)) {
        logger.debug(s"Transition from $current to $newState")
        persistState()
        current.stop(newState.term)
        newState.begin()
      }
      current = state
    }
    logger.trace("State is {}", current)
  }

  def persistState() = {
    val st = state
    if (st != Stopped) {
      storage.saveVote(Vote(st.term, st.votedFor.get().getOrElse("")))
    }
  }

  private def changeState(current: State, newState: State) = currentState.compareAndSet(current, newState)

  def term(): Term = state.term

  def stop(): Unit = {
    become(Stopped)
  }

  def stats(): ConsensusStats = ConsensusStats(term, currentState.toString, currentState.get().stats())

}
