package ckite

import ckite.rpc.RequestVote
import java.util.concurrent.atomic.AtomicReference
import org.slf4j.MDC
import ckite.util.Logging
import ckite.rpc.WriteCommand
import ckite.rpc.AppendEntriesResponse
import ckite.rpc.AppendEntries
import java.util.concurrent.Executors
import ckite.rpc.EnterJointConsensus
import ckite.rpc.LeaveJointConsensus
import ckite.rpc.MajorityJointConsensus
import ckite.rpc.RequestVoteResponse
import ckite.statemachine.StateMachine
import com.typesafe.config.Config
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import ckite.exception.WaitForLeaderTimedOutException
import scala.util.Success
import java.util.concurrent.TimeoutException
import java.util.concurrent.Callable
import ckite.executions.Executions
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
import ckite.rpc.EnterJointConsensus
import ckite.rlog.Snapshot
import scala.util.control.Breaks._
import java.util.concurrent.locks.ReentrantLock
import ckite.states.ReachMajorities


class Cluster(stateMachine: StateMachine, val configuration: Configuration) extends Logging {

  val db = DBMaker.newFileDB(file(configuration.dataDir)).mmapFileEnable().transactionDisable().closeOnJvmShutdown().make()
  
  val local = new LocalMember(this, configuration.localBinding)
  val InitialTerm = local.term
  val consensusMembership = new AtomicReference[Membership](new SimpleMembership(None,Seq()))
  val rlog = new RLog(this, stateMachine)
  val leaderPromise = new AtomicReference[Promise[Member]](Promise[Member]())
  
  val heartbeaterExecutor = new ThreadPoolExecutor(0, configuration.heartbeatsWorkers,
                                      10L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("HeartbeaterWorker", true))
  
  val electionExecutor = new ThreadPoolExecutor(0, configuration.electionWorkers,
                                      15L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("ElectionWorker", true))
  
  val scheduledElectionTimeoutExecutor = Executors.newScheduledThreadPool(1, new NamedPoolThreadFactory("ElectionTimeoutWorker", true))
  
  val replicatorExecutor = new ThreadPoolExecutor(0, configuration.replicationWorkers,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue[Runnable](),
                                      new NamedPoolThreadFactory("ReplicatorWorker", true))

  val waitForLeaderTimeout = Duration(configuration.waitForLeaderTimeout, TimeUnit.MILLISECONDS)
  
  
  def start = inContext {
    LOG.info("Start CKite Cluster")
    if (configuration.dynamicBootstrap) startDynamic else startStatic
  }

  //Members are known from the beginning
  private def startStatic = {
    val currentMembership = consensusMembership.get()
    if (currentMembership.allMembers.isEmpty) {
    	consensusMembership.set(new SimpleMembership(Some(local), configuration.membersBindings.map(binding => new RemoteMember(this, binding))))
    }
    local becomeFollower InitialTerm
  }

  //Members are seeds to discover the Leader and hence the Cluster
  private def startDynamic = {
    val dynaMembership = createSimpleConsensusMembership(Seq(local.id))
    consensusMembership.set(dynaMembership)
    breakable {
      local becomeFollower InitialTerm
      for (remoteMemberBinding <- configuration.membersBindings) {
        consensusMembership.set(createSimpleConsensusMembership(Seq(local.id, remoteMemberBinding)))
        val remoteMember = obtainRemoteMember(remoteMemberBinding).get
        val response = remoteMember.getMembers()
        if (response.success) {
          consensusMembership.set(createSimpleConsensusMembership(response.members :+ local.id))
          if (response.members.contains(local.id)) {
            LOG.info("I'm already part of the Cluster")
            break
          }
          val joinResponse = remoteMember.join(local.id)
          if (joinResponse.success) {
            LOG.info("Join request was succesful. Waiting for confirmation to be part of the Cluster.")
            break
          }
        }
      }
      //dynamicBootstrap fail to join. I'm the only one?
      consensusMembership.set(dynaMembership)
    }
  }
  
  def stop = inContext {
    LOG.info("Stop CKite Cluster")
    local stop
  }

  def on(requestVote: RequestVote):RequestVoteResponse = inContext {
    LOG.debug(s"RequestVote received: $requestVote")
    if (obtainRemoteMember(requestVote.memberId).isEmpty) {
      LOG.warn(s"Reject vote to member ${requestVote.memberId} who is not present in the Cluster")
      return RequestVoteResponse(local term, false)
    }
    local on requestVote
  }

  def on(appendEntries: AppendEntries) = inContext {
    local on appendEntries
  }

  def on[T](command: Command): T = havingLeader {
    inContext {
      LOG.debug(s"Command received: $command")
      local.on[T](command)
    }
  }

  def broadcastHeartbeats(term: Int) = {
    if (term == local.term) {
      val remoteMembers = membership.remoteMembers
      LOG.trace(s"Broadcasting heartbeats to $remoteMembers")
      remoteMembers.foreach { member =>
        heartbeaterExecutor.execute(() => {
          inContext {
            member.sendHeartbeat(term)
          }
        })
      }
    }
  }
  
  def onLocal(readCommand: ReadCommand) = {
    rlog execute readCommand
  }

  def collectVotes: Seq[Member] = {
    if (!hasRemoteMembers) return Seq()
    val execution = Executions.newExecution().withExecutor(electionExecutor)
    membership.remoteMembers.foreach { remoteMember =>
      execution.withTask(() => {
        inContext {
          (remoteMember, remoteMember requestVote)
        }
      })
    }
    //when in JointConsensus we need quorum on both cluster memberships (old and new)
    //TODO: refactor
    val membersVotes = execution.withTimeout(configuration.collectVotesTimeout, TimeUnit.MILLISECONDS)
      .withExpectedResults(1, new ReachMajorities(this)).execute[(Member, Boolean)]()
    val votesGrantedMembers = membersVotes.asScala.filter { memberVote => memberVote._2 }.map { voteGranted => voteGranted._1 }
    votesGrantedMembers.toSeq
  }

  def forwardToLeader[T](command: Command): T = withLeader { leader => 
    inContext {
    	LOG.debug(s"Forward command $command")
    	leader.forwardCommand[T](command)
    }
  }

  def updateLeader(memberId: String): Boolean = leaderPromise.synchronized {
    inContext {
      val newLeader = obtainMember(memberId)
      val promise = leaderPromise.get()
      if (!promise.isCompleted || promise.future.value.get.get != newLeader.get) {
        leaderPromise.get().success(newLeader.get) //complete current promise for operations waiting for it
        leaderPromise.set(Promise.successful(newLeader.get)) //kept promise for subsequent leader 
        true
      } else false
    }
  }

  def setNoLeader = leaderPromise.synchronized {
    inContext {
      if (leaderPromise.get().isCompleted) {
        leaderPromise.set(Promise[Member]())
      }
    }
  }

  def addMember(memberBinding: String) = {
    val newMemberBindings = membership.allBindings :+ memberBinding
    on[Boolean](EnterJointConsensus(newMemberBindings.toList))
  }
  
  def removeMember(memberBinding: String) = {
    val newMemberBindings = membership.allBindings diff Seq(memberBinding)
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
	consensusMembership.set(createSimpleConsensusMembership(leaveJointConsensus.bindings))
	LOG.info(s"Membership ${consensusMembership.get()}")
  }

  private def createSimpleConsensusMembership(bindings: Seq[String]): SimpleMembership = {
    val localOption = if (bindings.contains(local.id)) Some(local) else None
    val bindingsWithoutLocal = bindings diff Seq(local.id) toSet
    
    new SimpleMembership(localOption, bindingsWithoutLocal.toSeq.map { binding => obtainRemoteMember(binding).getOrElse(new RemoteMember(this, binding))})
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
  
  def installSnapshot(snapshot: Snapshot): Boolean = inContext {
    LOG.info("InstallSnapshot received")
    rlog.installSnapshot(snapshot)
  }
  
  def getMembers(): Seq[String] = withLeader { leader =>
     if (leader == local) membership.allBindings else leader.asInstanceOf[RemoteMember].getMembers().members
  }
  
  def isActiveMember(memberId: String): Boolean = membership.allBindings.contains(memberId)
  
  def obtainRemoteMember(memberId: String): Option[RemoteMember] = (membership.remoteMembers).find { _.id == memberId }
  
  def anyLeader = leader != None

  def majority = membership.majority

  def reachMajority(members: Seq[Member]): Boolean = membership.reachMajority(members)
  
  def updateContextInfo = {
    MDC.put("binding", local.id)
    MDC.put("term", local.term.toString)
    MDC.put("leader", leader.toString)
  }
  
  def inContext[T](f: => T): T = {
    updateContextInfo
    val result = f
    updateContextInfo
    result
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

  private def file(dataDir: String): File = {
    val dir = new File(dataDir)
    dir.mkdirs()
    val file = new File(dir, "ckite")
    file
  }

}