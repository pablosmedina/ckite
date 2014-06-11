package ckite

import org.mapdb.DB
import ckite.rpc.RequestVote
import ckite.rpc.RequestVoteResponse
import ckite.states.Follower
import ckite.states.Leader
import ckite.states.Candidate
import ckite.rpc.Command
import ckite.states.State
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.AppendEntries
import java.util.concurrent.atomic.AtomicReference
import ckite.states.Starter
import ckite.states.Stopped
import java.util.concurrent.locks.ReentrantLock
import org.mapdb.DBMaker
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.Promise

class LocalMember(cluster: Cluster, binding: String) extends Member(binding) {

  val state = new AtomicReference[State](Starter)
  
  val db = DBMaker.newFileDB(file(cluster.configuration.dataDir)).mmapFileEnable().closeOnJvmShutdown().make()
  val persistedTerm = db.getAtomicInteger("term")
  val votedFor = db.getAtomicString("votedFor")
  
  val lock = new ReentrantLock()

  def term(): Int = currentState.term

  def persistState() = {
    val st = currentState
    if (st != Stopped) {
    	persistedTerm.set(st.term)
    	votedFor.set(st.votedFor.get().getOrElse(""))
    	db.commit()
    }
  }

  def becomeLeader(term: Int) = become(new Leader(cluster, term, currentLeaderPromise), term)

  def becomeCandidate(term: Int) = become(new Candidate(cluster, term, currentLeaderPromiseForCandidate), term)

  def becomeFollower(term: Int, passive: Boolean = false, leaderPromise: Promise[Member] = Promise[Member](), vote: Option[String] = None) = become(new Follower(cluster, passive, term, leaderPromise, vote), term)

  def becomePassiveFollower(term: Int): Unit = becomeFollower(term, true)
  
  def becomeStarter = changeState(Starter, Starter)
  
  private def currentLeaderPromise = {
    currentState.leaderPromise
  }
  
  private def currentLeaderPromiseForCandidate = {
    val promise = currentLeaderPromise
    if (!promise.isCompleted) promise else Promise[Member]()
  }

  private def become(newState: State, term: Int) = {
    LOG.trace("Trying to become {}",newState)
    var current = currentState
    //stops when current == newState or current.term < newState.term
    while(current.canTransitionTo(newState)) {
      if (changeState(current, newState)) {
    	  LOG.debug(s"Transition from $current to $newState")
    	  cluster.local.persistState
    	  current stop term
    	  newState begin
      }
      current = currentState
    }
    LOG.trace("State is {}",current)
  }
  
  //must operate on a fixed term and give at most one vote per term
  def on(requestVote: RequestVote): Future[RequestVoteResponse] = {
    if (requestVote.term < term) {
      LOG.debug(s"Rejecting vote to old candidate: ${requestVote}")
      return Future.successful(RequestVoteResponse(term, false))
    }
    currentState on requestVote
  }

  def on(appendEntries: AppendEntries): Future[AppendEntriesResponse] = currentState on appendEntries

  def on[T](command: Command): Future[T] = currentState.on[T](command)

  def on(jointConsensusCommited: MajorityJointConsensus) = currentState on jointConsensusCommited

  override def forwardCommand[T](command: Command): Future[T] = on(command)

  def stepDown(term: Int, leaderId: Option[String] = None) = currentState.stepDown(term, leaderId)

  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {
    currentState.onAppendEntriesResponse(member, request, response)
  }

  def currentState = state.get()

  private def changeState(current: State, newState: State) = state.compareAndSet(current, newState)

  def stop: Unit = {
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