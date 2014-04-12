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
import ckite.rpc.EnterJointConsensus
import ckite.rlog.Snapshot
import scala.util.control.Breaks._
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.util.Try


class Cluster(stateMachine: StateMachine, val configuration: Configuration) extends Logging {

  val db = DBMaker.newFileDB(file(configuration.dataDir)).mmapFileEnable().transactionDisable().closeOnJvmShutdown().make()
  
  val local = new LocalMember(this, configuration.localBinding)
  val consensusMembership = new AtomicReference[Membership](EmptyMembership)
  val rlog = new RLog(this, stateMachine)
  
  val leaderPromise = new AtomicReference[Promise[Member]](Promise[Member]())

  val appendEntriesExecutionContext = ExecutionContext.fromExecutor(new ThreadPoolExecutor(0, configuration.appendEntriesWorkers,
    10L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("AppendEntriesWorker", true)))

  val electionExecutionContext = ExecutionContext.fromExecutor(new ThreadPoolExecutor(0, configuration.electionWorkers,
    15L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new NamedPoolThreadFactory("ElectionWorker", true)))
  
  val scheduledElectionTimeoutExecutor = Executors.newScheduledThreadPool(1, new NamedPoolThreadFactory("ElectionTimeoutWorker", true))
  
  val waitForLeaderTimeout = configuration.waitForLeaderTimeout millis
  
  
  def start = inContext {
    LOG.info("Start CKite Cluster")
   
    local becomeStarter
    
    if (configuration.dynamicBootstrap) startDynamic else startStatic
  }

  //Members are known from the beginning
  private def startStatic = {
    val currentMembership = consensusMembership.get()
    if (currentMembership.allMembers.isEmpty) {
    	consensusMembership.set(new SimpleConsensus(Some(local), configuration.membersBindings.map(binding => new RemoteMember(this, binding))))
    }
    local becomeFollower
  }

  //Members are seeds to discover the Leader and hence the Cluster
  private def startDynamic = {
    val dynaMembership = simpleConsensus(Seq(local.id))
    consensusMembership.set(dynaMembership)
    breakable {
      local becomeFollower
      
      for (remoteMemberBinding <- configuration.membersBindings) {
        consensusMembership.set(simpleConsensus(Seq(local.id, remoteMemberBinding)))
        val remoteMember = obtainRemoteMember(remoteMemberBinding).get
        val response = remoteMember.getMembers()
        if (response.success) {
          consensusMembership.set(simpleConsensus(response.members :+ local.id))
          if (response.members.contains(local.id)) {
            LOG.debug("I'm already part of the Cluster")
            break
          }
          val joinResponse = remoteMember.join(local.id)
          if (joinResponse.success) {
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
    obtainRemoteMember(requestVote.memberId).map { remoteMember => 
      local on requestVote
    }.getOrElse {
      LOG.warn(s"Reject vote to member ${requestVote.memberId} who is not present in the Cluster")
      RequestVoteResponse(local term, false)
    }
  }

  def on(appendEntries: AppendEntries) = inContext {
    local on appendEntries
  }

  def on[T](command: Command): T = inContext {
    havingLeader {
      LOG.debug(s"Command received: $command")
      local.on[T](command)
    }
  }

  def broadcastAppendEntries(term: Int)(implicit context: ExecutionContext = appendEntriesExecutionContext) = {
    if (term == local.term) {
      membership.remoteMembers foreach { member =>
        future {
          inContext {
            member sendAppendEntries term
          }
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
    	  inContext {
    		  (remoteMember, remoteMember requestVote)
    	  }
      } onSuccess { case (member, vote) =>
        votes.put(member, vote)
        val grantedVotes = votes.asScala.filter{ tuple =>  tuple._2 }.keySet.toSeq
        val rejectedVotes = votes.asScala.filterNot{ tuple =>  tuple._2 }.keySet.toSeq
        if (membership.reachMajority(grantedVotes) || 
            membership.reachAnyMajority(rejectedVotes) || 
            membership.allMembers.size == votes.size()) 
        	promise.trySuccess(grantedVotes)
      }
    }
    Try {
    	Await.result(promise.future, configuration.collectVotesTimeout millis)
    } getOrElse {
      votes.asScala.filter{ tuple =>  tuple._2 }.keySet.toSeq
    }
  }

  def forwardToLeader[T](command: Command): T = inContext {
    withLeader { leader =>
      LOG.debug(s"Forward command $command")
      leader.forwardCommand[T](command)
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
    consensusMembership.set(JointConsensus(currentMembership, simpleConsensus(enterJointConsensus.newBindings)))
    LOG.info(s"Membership ${consensusMembership.get()}")
  }
  
  //EnterJointConsensus reached quorum. Send LeaveJointConsensus If I'm the Leader to notify the new membership.
  def on(majorityJointConsensus: MajorityJointConsensus) = {
    local on majorityJointConsensus
  }

  //LeaveJointConsensus received. A new membership has been set. Switch to SimpleConsensus or shutdown If no longer part of the cluster.
  def apply(leaveJointConsensus: LeaveJointConsensus) = {
    LOG.info(s"Leaving JointConsensus")
	consensusMembership.set(simpleConsensus(leaveJointConsensus.bindings))
	LOG.info(s"Membership ${consensusMembership.get()}")
  }

  private def simpleConsensus(bindings: Seq[String]): SimpleConsensus = {
    val localOption = if (bindings.contains(local.id)) Some(local) else None
    val bindingsWithoutLocal = bindings diff Seq(local.id).toSet.toSeq
    
    SimpleConsensus(localOption, bindingsWithoutLocal.map { binding => obtainRemoteMember(binding).getOrElse(new RemoteMember(this, binding))})
  }

  def membership = consensusMembership.get()

  def hasRemoteMembers = !membership.remoteMembers.isEmpty

  def updateLeader(leaderId: String): Boolean = leaderPromise.synchronized {
    inContext {
      val newLeader = obtainMember(leaderId)
      val promise = leaderPromise.get()
      val isNew = !promise.isCompleted || promise.future.value.get.get != newLeader.get
      if (isNew) {
        leaderPromise.get().success(newLeader.get) //complete current promise for operations waiting for it
        leaderPromise.set(Promise.successful(newLeader.get)) //kept promise for subsequent leader 
      }
      isNew
    }
  }

  def setNoLeader = leaderPromise.synchronized {
    inContext {
      if (leaderPromise.get().isCompleted) {
        leaderPromise.set(Promise[Member]())
      }
    }
  }
  
  def awaitLeader: Member = {
    try {
      Await.result(leaderPromise.get().future, waitForLeaderTimeout)
    } catch {
      case e: TimeoutException => {
        LOG.warn(s"Wait for Leader timed out after $waitForLeaderTimeout")
        throw new LeaderTimeoutException(e)
      }
    }
  }

  def leader: Option[Member] = {
    val promise = leaderPromise.get()
    if (promise.isCompleted) Some(promise.future.value.get.get) else None
  }
  
  def installSnapshot(snapshot: Snapshot): Boolean = inContext {
    LOG.debug("InstallSnapshot received")
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