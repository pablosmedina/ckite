package ckite.states

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ ConcurrentHashMap, SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import ckite._
import ckite.rpc.LogEntry.Term
import ckite.rpc._
import ckite.util.CKiteConversions._
import ckite.util.CustomThreadFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future, Promise }
import scala.util.{ Failure, Success, Try }

case class Candidate(consensus: Consensus, membership: Membership, log: RLog, term: Term, leaderAnnouncer: LeaderAnnouncer) extends State(Some(membership.myId)) {

  private val electionWorker = new ThreadPoolExecutor(1, 1,
    60L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    CustomThreadFactory(s"CandidateElection-worker-${membership.myId}"))

  private val runningElection = new AtomicReference[java.util.concurrent.Future[_]]()
  private val votes = new ConcurrentHashMap[String, Boolean]()
  private val maxVotesExpected = membership.members.size

  override def begin() = {
    logger.debug(s"Start election")
    startElection()
  }

  private def startElection() = {
    val electionTask: Runnable = () ⇒ {
      election()
    }
    runningElection.set(electionWorker.submit(electionTask))
  }

  private def election() = {
    val votes = collectVotes()
    logger.debug(s"Got ${votes.size} votes")
    if (membership.reachQuorum(votes)) wonElection() else lostElection()
  }

  def lostElection() {
    logger.info(s"Not enough votes to be a Leader")
    consensus.becomeFollower(term = term, vote = Some(membership.myId)) //voted for my self when Candidate
  }

  def wonElection() {
    logger.info(s"Won the election. Will become Leader...")
    consensus.becomeLeader(term)
  }

  private def collectVotes(): Set[String] = {
    if (!membership.hasRemoteMembers) return Set(membership.myId)
    val votesPromise = Promise[Set[String]]()

    voteForMyself()

    val lastLogEntry = log.lastEntry
    membership.remoteMembers.foreach { remoteMember ⇒
      Future {
        (remoteMember.id, requestVote(remoteMember, lastLogEntry))
      } onComplete {
        case Success((member, vote)) ⇒ {
          vote map { granted ⇒
            votes.put(member, granted)
            val grantedVotes = votes.asScala.filter { tuple ⇒ tuple._2 }.keySet.toSet
            val rejectedVotes = votes.asScala.filterNot { tuple ⇒ tuple._2 }.keySet.toSet
            if (membership.reachQuorum(grantedVotes) ||
              membership.reachSomeQuorum(rejectedVotes) ||
              maxVotesExpected == votes.size())
              votesPromise.trySuccess(grantedVotes)
          }
        }
        case Failure(reason) ⇒ {
          logger.error("failure collecting votes", reason)
        }
      }
    }
    Try {
      Await.result(votesPromise.future, consensus.configuration.collectVotesTimeout millis) //TODO: Refactor me
    } getOrElse {
      votes.asScala.filter { tuple ⇒ tuple._2 }.keySet.toSet
    }
  }

  private def requestVote(remoteMember: RemoteMember, lastLogEntry: Option[LogEntry]): Future[Boolean] = {
    logger.debug(s"Requesting vote to ${remoteMember.id}")
    remoteMember.sendRequestVote(lastLogEntry match {
      case None        ⇒ RequestVote(membership.myId, term)
      case Some(entry) ⇒ RequestVote(membership.myId, term, entry.index, entry.term)
    }).map { voteResponse ⇒
      logger.debug(s"Got $voteResponse from ${remoteMember.id}")
      voteResponse.granted && voteResponse.currentTerm == term
    } recover {
      case reason: Throwable ⇒
        logger.debug(s"Error requesting vote: ${reason.getMessage()}")
        false
    }
  }

  private def voteForMyself() {
    //This is in conflict with the vote set upon Candidate creation
    if (membership.members.contains(membership.myId)) {
      votes.put(membership.myId, true) //Can Local vote???
    }
  }

  private def abortElection() = {
    logger.debug("Abort Election")
    val future = runningElection.get()
    if (future != null) future.cancel(true)
  }

  override def canTransitionTo(newState: State) = {
    newState match {
      case leader: Leader     ⇒ leader.term == term
      case follower: Follower ⇒ follower.term >= term //in case of split vote or being an old candidate
      case _                  ⇒ newState.term > term
    }
  }

  override def onAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    appendEntries.term match {
      case leaderTerm if leaderTerm < term  ⇒ rejectOldLeader(appendEntries)
      case leaderTerm if leaderTerm >= term ⇒ stepDownAndPropagate(appendEntries)
    }
  }

  override def stepDownAndPropagate(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    logger.debug("Leader already elected in term[{}]", appendEntries.term)
    abortElection()
    super.stepDownAndPropagate(appendEntries)
  }

  override def stepDownAndPropagate(requestVote: RequestVote): Future[RequestVoteResponse] = {
    abortElection()
    super.stepDownAndPropagate(requestVote)
  }

  override def onRequestVote(requestVote: RequestVote): Future[RequestVoteResponse] = {
    requestVote.term match {
      case candidateTerm if candidateTerm < term  ⇒ rejectOldCandidate(requestVote.memberId)
      case candidateTerm if candidateTerm == term ⇒ rejectVote(requestVote.memberId, "contender candidate of the same term")
      case candidateTerm if candidateTerm > term  ⇒ stepDownAndPropagate(requestVote)
    }
  }

  override def onInstallSnapshot(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = {
    Future.successful(InstallSnapshotResponse(false))
  }

  override def onCommand[T](command: Command): Future[T] = {
    leaderAnnouncer.onLeader(_.forwardCommand[T](command))
  }

  override def toString = s"Candidate[$term]"

}
