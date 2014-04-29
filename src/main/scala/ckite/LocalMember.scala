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

class LocalMember(cluster: Cluster, binding: String) extends Member(binding) {

  val state = new AtomicReference[State](Starter)
   val db = DBMaker.newFileDB(file(cluster.configuration.dataDir)).mmapFileEnable().closeOnJvmShutdown().make()
  val currentTerm = db.getAtomicInteger("term")
  val transientTerm = new AtomicInteger(currentTerm.intValue())
  val votedFor = db.getAtomicString("votedFor")

  val lock = new ReentrantLock()
  
  def term(): Int = transientTerm.intValue()

  def voteForMyself = votedFor.set(id)

  def on(requestVote: RequestVote): RequestVoteResponse = locked {
    if (requestVote.term < term) {
      LOG.debug(s"Rejecting vote to old candidate: ${requestVote}")
      return RequestVoteResponse(term, false)
    }
    currentState on requestVote
  }

  def updateTermIfNeeded(receivedTerm: Int) = locked {
    cluster.inContext {
    	if (receivedTerm > term) {
    		LOG.debug(s"New term detected. Moving from ${term} to ${receivedTerm}.")
    		votedFor.set("")
    		currentTerm.set(receivedTerm)
    		db.commit()
    		transientTerm.set(currentTerm.intValue())
    	}
    }
  }

  def incrementTerm = cluster.inContext {
    val t = currentTerm.incrementAndGet()
    db.commit()
    transientTerm.set(t)
    t
  }

  def becomeLeader(term: Int) = become(new Leader(cluster), term)

  def becomeCandidate(term: Int) = become(new Candidate(cluster), term)

  def becomeFollower(term: Int) = become(new Follower(cluster), term)
  
  def becomeFollower:Unit = becomeFollower(term())
  
  def becomeStarter = changeState(Starter)
  
  private def become(newState: State, term: Int): Unit = locked {
    if (currentState.canTransition) {
      if (cluster.isActiveMember(id)) {
        LOG.debug(s"Transition from $state to $newState")
        currentState stop

        changeState(newState)
        
        newState begin term

      } else {
        LOG.warn(s"No longer part of the Cluster. Shutdown!")
        cluster.stop
      }
    }
  }

  def on(appendEntries: AppendEntries): AppendEntriesResponse = currentState on appendEntries

  def on[T](command: Command): T = currentState.on[T](command)

  def on(jointConsensusCommited: MajorityJointConsensus) = currentState on jointConsensusCommited

  override def forwardCommand[T](command: Command): T = on(command)
  
  def stepDown(term: Int, leaderId: Option[String] = None) = currentState.stepDown(term, leaderId)
  
  def onAppendEntriesResponse(member: RemoteMember, request: AppendEntries, response: AppendEntriesResponse) = {
    currentState.onAppendEntriesResponse(member, request, response)
  }
  
  def currentState = state.get()

  private def changeState(newState: State) = state.set(newState)

  def stop: Unit = {
    become(Stopped, cluster.local.term)
    db.close()
  }
  
  def locked[T](f: => T): T = {
       lock.lock()
       try {
         f
       } finally {
         lock.unlock()
       }
  }
  
    private def file(dataDir: String): File = {
    val dir = new File(dataDir)
    dir.mkdirs()
    val file = new File(dir, "ckite")
    file
  }
}