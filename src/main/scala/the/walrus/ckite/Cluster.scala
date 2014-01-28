package the.walrus.ckite

import the.walrus.ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import org.slf4j.MDC
import the.walrus.ckite.util.Logging
import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries
import java.util.concurrent.Executors
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rpc.LeaveJointConsensus
import the.walrus.ckite.rpc.MajorityJointConsensus
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.statemachine.StateMachine
import com.typesafe.config.Config
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import the.walrus.ckite.exception.WaitForLeaderTimedOutException
import scala.util.Success
import java.util.concurrent.TimeoutException
import java.util.concurrent.Callable
import the.walrus.ckite.executions.Executions
import the.walrus.ckite.states.MajoritiesExpected
import scala.collection.JavaConversions._
import the.walrus.ckite.util.CKiteConversions._
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command
import org.mapdb.DBMaker
import java.io.File
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.Executors.DefaultThreadFactory
import com.twitter.concurrent.NamedPoolThreadFactory
import the.walrus.ckite.rpc.EnterJointConsensus
import the.walrus.ckite.rlog.Snapshot

class Cluster(stateMachine: StateMachine, val configuration: Configuration) extends Logging {

  implicit val aCluster = this

  val db = DBMaker.newFileDB(file(configuration.dataDir)).mmapFileEnable().transactionDisable().closeOnJvmShutdown().make()
  
  val local = new LocalMember(this, configuration.localBinding, db)
  val InitialTerm = local.term
  val consensusMembership = new AtomicReference[Membership]()
  val rlog = new RLog(this, stateMachine, db)
  val leaderPromise = new AtomicReference[Promise[Member]](Promise[Member]())
  
  val heartbeaterExecutor = new ThreadPoolExecutor(0, 50,
                                      10L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("HeartbeaterWorker", true))
  
  val electionExecutor = new ThreadPoolExecutor(0, 50,
                                      15L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("ElectionWorker", true))
  
  val scheduledElectionTimeoutExecutor = Executors.newScheduledThreadPool(1, new NamedPoolThreadFactory("ElectionTimeout", true))
  
  val replicatorExecutor = new ThreadPoolExecutor(0, 50,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("ReplicatorWorker", true))

  val waitForLeaderTimeout = Duration(configuration.waitForLeaderTimeout, TimeUnit.MILLISECONDS)
  
  def start = {
    updateContextInfo
    LOG.info("Start CKite Cluster")
    consensusMembership.set(new SimpleConsensusMembership(local, configuration.membersBindings.map(binding => new RemoteMember(this, binding))))
    local becomeFollower InitialTerm
  }

  def stop = {
    LOG.info("Stop CKite Cluster")
    local stop
  }

  def on(requestVote: RequestVote) = {
    updateContextInfo
    LOG.debug(s"RequestVote received: $requestVote")
    if (obtainMember(requestVote.memberId).isEmpty) {
      LOG.warn(s"Reject vote to member ${requestVote.memberId} who is not present in the Cluster")
      RequestVoteResponse(local term, false)
    }
    local on requestVote
  }

  def on(appendEntries: AppendEntries) = {
    updateContextInfo
    local on appendEntries
  }

  def on[T](command: Command): T = {
    awaitLeader
    updateContextInfo
    LOG.debug(s"Command received: $command")
    command match {
      case write: WriteCommand => synchronized { local.on[T](write) }
      case read: ReadCommand => local.on[T](read)
    }
  }

  def broadcastHeartbeats(term: Int) = synchronized {
    val remoteMembers = membership.remoteMembers
    LOG.trace(s"Broadcasting heartbeats to $remoteMembers")
    remoteMembers.foreach { member =>
      heartbeaterExecutor.execute (() => {
        updateContextInfo
        member.sendHeartbeat(term)
      })
    }
  }

  def onLocal(readCommand: ReadCommand) = {
    rlog execute readCommand
  }

  def collectVotes: Seq[Member] = {
    if (hasRemoteMembers) {
      val execution = Executions.newExecution().withExecutor(electionExecutor)
      consensusMembership.get.remoteMembers.foreach { member =>
        execution.withTask(new Callable[(Member, Boolean)] {
		          override def call() = {
		          updateContextInfo
		          (member, member requestVote)
		        }
        }
        )
      }
      val expectedResults = membership.majoritiesCount
      val rawResults = execution.withTimeout(configuration.collectVotesTimeout, TimeUnit.MILLISECONDS)
        .withExpectedResults(expectedResults, new MajoritiesExpected(this)).execute[(Member, Boolean)]()

      val results: Iterable[(Member, Boolean)] = rawResults
      val mapres = results.filter { result => result._2 }.map { result => result._1 }
      mapres.toSeq
    } else {
      Seq()
    }
  }

  def forwardToLeader[T](command: Command): T = {
    awaitLeader.forwardCommand[T](command)
  }

  def updateLeader(memberId: String): Boolean = leaderPromise.synchronized {
    val newLeader = obtainMember(memberId)
    val promise = leaderPromise.get()
    if (!promise.isCompleted || promise.future.value.get.get != newLeader.get) {
      leaderPromise.get().success(newLeader.get) //complete current promise for operations waiting for it
      leaderPromise.set(Promise.successful(newLeader.get)) //kept promise for subsequent leader 
      updateContextInfo
      true
    } else false
  }

  def setNoLeader = leaderPromise.synchronized {
    if (leaderPromise.get().isCompleted) {
      leaderPromise.set(Promise[Member]())
    }
    updateContextInfo
  }

  def anyLeader = leader != None

  def majority = membership.majority

  def reachMajority(votes: Seq[Member]): Boolean = membership.reachMajority(votes)

  def updateContextInfo = {
    MDC.put("term", local.term.toString)
    MDC.put("leader", leader.toString)
  }
  
  def addMember(memberBinding: String) = {
    val newMemberBindings = consensusMembership.get().allMembers.map {member => member.id } :+ memberBinding
    on(EnterJointConsensus(newMemberBindings.toList))
  }
  
  def removeMember(memberBinding: String) = {
    val newMemberBindings = consensusMembership.get().allMembers.map {member => member.id } diff memberBinding
    on(EnterJointConsensus(newMemberBindings.toList))
  }
  
  //EnterJointConsensus received. Switch membership to JointConsensus
  def apply(enterJointConsensus: EnterJointConsensus) = {
    LOG.info(s"Entering in JointConsensus")
    val currentMembership = consensusMembership.get()
    consensusMembership.set(new JointConsensusMembership(currentMembership, createSimpleConsensusMembership(enterJointConsensus.newBindings)))
    LOG.info(s"Membership ${consensusMembership.get()}")
  }
  
  //EnterJointConsensus reached quorum. Send LeaveJointConsensus If I'm the Leader to notify the new membership.
  def on(majorityJointConsensus: MajorityJointConsensus) = {
    local on majorityJointConsensus
  }

  //LeaveJointConsensus received. A new membership has been set. Switch to SimpleConsensus or shutdown If no longer part of the cluster.
  def apply(leaveJointConsensus: LeaveJointConsensus) = {
    LOG.info(s"Leaving JointConsensus")
    //Check If I'm part of the new Cluster and shutdown if not
    if (!leaveJointConsensus.bindings.contains(local.id)) {
      stop
    } else {
    	consensusMembership.set(createSimpleConsensusMembership(leaveJointConsensus.bindings))
    	LOG.info(s"Membership ${consensusMembership.get()}")
    }
  }

  private def createSimpleConsensusMembership(bindings: Seq[String]): SimpleConsensusMembership = {
    val bindingsWithoutLocal = bindings diff local.id
    new SimpleConsensusMembership(local, bindingsWithoutLocal.map { binding => obtainRemoteMember(binding).getOrElse(new RemoteMember(this, binding))})
  }

  def membership = consensusMembership.get()

  def hasRemoteMembers = !membership.remoteMembers.isEmpty

  def awaitLeader: Member = {
    try {
      Await.result(leaderPromise.get().future, waitForLeaderTimeout)
    } catch {
      case e: TimeoutException => throw new WaitForLeaderTimedOutException(e)
    }
  }

  def leader: Option[Member] = {
    val promise = leaderPromise.get()
    if (promise.isCompleted) Some(promise.future.value.get.get) else None
  }
  
  private def obtainMember(memberId: String): Option[Member] = (membership.allMembers).find { _.id == memberId }
  private def obtainRemoteMember(memberId: String): Option[RemoteMember] = (membership.remoteMembers).find { _.id == memberId }
  
  private def file(dataDir: String): File = {
    val dir = new File(dataDir)
    dir.mkdirs()
    val file = new File(dir, "ckite")
    file
  }
  
  def installSnapshot(snapshot: Snapshot): Boolean = {
    LOG.info("InstallSnapshot received")
    rlog.installSnapshot(snapshot)
  }

}