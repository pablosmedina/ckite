package ckite

import java.io.File
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import ckite.rpc.{ AppendEntries, AppendEntriesResponse, Command, MajorityJointConsensus, RequestVote, RequestVoteResponse }
import ckite.states.{ Candidate, Follower, Leader, Starter, State, Stopped }
import org.mapdb.DBMaker

import scala.concurrent.{ Future, Promise }

class LocalMember(cluster: Cluster, binding: String) extends Member(binding) {

  val state = new AtomicReference[State](Starter)

  val db = DBMaker.newFileDB(file(cluster.configuration.dataDir)).mmapFileEnable().closeOnJvmShutdown().make()
  val persistedTerm = db.getAtomicInteger("term")
  val votedFor = db.getAtomicString("votedFor")

  val lock = new ReentrantLock()

  def becomeLeader(term: Int) = become(new Leader(cluster, term, currentLeaderPromise), term)

  private def currentLeaderPromise = {
    currentState.leaderPromise
  }

  private def become(newState: State, term: Int) = {
    log.trace("Trying to become {}", newState)
    var current = currentState
    //stops when current == newState or current.term < newState.term
    while (current.canTransitionTo(newState)) {
      if (changeState(current, newState)) {
        log.debug(s"Transition from $current to $newState")
        cluster.local.persistState
        current stop term
        newState begin
      }
      current = currentState
    }
    log.trace("State is {}", current)
  }

  def persistState() = {
    val st = currentState
    if (st != Stopped) {
      persistedTerm.set(st.term)
      votedFor.set(st.votedFor.get().getOrElse(""))
      db.commit()
    }
  }

  private def changeState(current: State, newState: State) = state.compareAndSet(current, newState)

  def becomeCandidate(term: Int) = become(new Candidate(cluster, term, currentLeaderPromiseForCandidate), term)

  private def currentLeaderPromiseForCandidate = {
    val promise = currentLeaderPromise
    if (!promise.isCompleted) promise else Promise[Member]()
  }

  def becomePassiveFollower(term: Int): Unit = becomeFollower(term, true)

  def becomeFollower(term: Int, passive: Boolean = false, leaderPromise: Promise[Member] = Promise[Member](), vote: Option[String] = None) = become(new Follower(cluster, passive, term, leaderPromise, vote), term)

  def becomeStarter = changeState(Starter, Starter)

  //must operate on a fixed term and give at most one vote per term
  def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    if (requestVote.term < term) {
      log.debug(s"Rejecting vote to old candidate: ${requestVote}")
      return Future.successful(RequestVoteResponse(term, false))
    }
    currentState on requestVote
  }

  def term(): Int = currentState.term

  def on(appendEntries: AppendEntries): Future[AppendEntriesResponse] = currentState on appendEntries

  def on(jointConsensusCommitted: MajorityJointConsensus) = currentState on jointConsensusCommitted

  override def forwardCommand[T](command: Command): Future[T] = on(command)

  def on[T](command: Command): Future[T] = currentState.on[T](command)

  def stepDown(term: Int, leaderId: Option[String] = None) = currentState.stepDown(term, leaderId)

  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {
    currentState.onAppendEntriesResponse(member, request, response)
  }

  def currentState = state.get()

  def stop(): Unit = {
    become(Stopped, Stopped.term)
    db.close()
  }

  private def file(dataDir: String): File = {
    val dir = new File(dataDir)
    dir.mkdirs()
    val file = new File(dir, "ckite")
    file
  }
}