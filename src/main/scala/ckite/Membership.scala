package ckite

import java.util.concurrent.atomic.AtomicReference

import ckite.rpc.LogEntry.Index
import ckite.rpc.{ Rpc, ClusterConfigurationCommand, JointConfiguration, NewConfiguration }
import ckite.util.Logging

trait ClusterConfiguration {
  /** The index in the Log where this ClusterConfiguration is located */
  def index: Index

  /** All the members included in this ClusterConfiguration. This can includes both new and old members */
  def members: Set[String]

  /** Checks if the given members forms a quorum in this ClusterConfiguration. */
  def reachQuorum(someMembers: Set[String]): Boolean

  /** Checks if the given members forms SOME quorum. Useful in the case of JointClusterConfiguration */
  def reachSomeQuorum(someMembers: Set[String]): Boolean
}

case class SingleClusterConfiguration(members: Set[String], index: Index = -1) extends ClusterConfiguration {
  private val quorum = (members.size / 2) + 1

  def reachQuorum(someMembers: Set[String]) = someMembers.intersect(members).size >= quorum

  def reachSomeQuorum(someMembers: Set[String]) = reachQuorum(someMembers)
}

case class JointClusterConfiguration(cold: SingleClusterConfiguration, cnew: SingleClusterConfiguration, index: Index) extends ClusterConfiguration {
  val members = cold.members ++ cnew.members

  def reachQuorum(someMembers: Set[String]) = cold.reachQuorum(someMembers) && cnew.reachQuorum(someMembers)

  def reachSomeQuorum(someMembers: Set[String]) = cold.reachQuorum(someMembers) || cnew.reachQuorum(someMembers)

  override def toString = s"JointClusterConfiguration(cold=${cold.members}, cnew=${cnew.members}, index= $index)"
}

object JointClusterConfiguration {
  implicit def fromMembersSetToSimpleClusterConfiguration(members: Set[String]): SingleClusterConfiguration = {
    SingleClusterConfiguration(members)
  }
}

object EmptyClusterConfiguration extends SingleClusterConfiguration(Set())

case class Membership(localMember: LocalMember, rpc: Rpc, configuration: Configuration) extends Logging {

  import ckite.JointClusterConfiguration._

  private val currentClusterConfiguration = new AtomicReference[ClusterConfiguration](EmptyClusterConfiguration)
  private val currentKnownMembers = new AtomicReference[Map[String, RemoteMember]](Map())

  register(configuration.memberBindings)

  def clusterConfiguration = currentClusterConfiguration.get()

  private def knownMembers = currentKnownMembers.get()

  def members = clusterConfiguration.members
  def remoteMembers = (clusterConfiguration.members - localMember.id()).map(member ⇒ knownMembers(member))
  def hasRemoteMembers = !remoteMembers.isEmpty

  def reachQuorum(someMembers: Set[String]) = clusterConfiguration.reachQuorum(someMembers)

  def reachSomeQuorum(someMembers: Set[String]) = clusterConfiguration.reachSomeQuorum(someMembers)

  def get(member: String): Option[RemoteMember] = {
    knownMembers.get(member).orElse {
      register(Set(member))
      knownMembers.get(member)
    }
  }

  def changeConfiguration(index: Index, clusterConfiguration: ClusterConfigurationCommand) = {
    if (happensBefore(index)) {
      clusterConfiguration match {
        case JointConfiguration(oldMembers, newMembers) ⇒ {
          //JointConfiguration received. Switch membership to JointClusterConfiguration
          transitionTo(JointClusterConfiguration(oldMembers, newMembers, index))
        }
        case NewConfiguration(members) ⇒ {
          //NewConfiguration received. A new membership has been set. Switch to SimpleClusterConfiguration or shutdown If no longer part of the cluster.
          transitionTo(SingleClusterConfiguration(members, index))
        }
      }
    }
  }

  def transitionTo(newClusterConfiguration: ClusterConfiguration) = {
    val newMembers = newClusterConfiguration.members.filterNot(member ⇒ knownMembers.contains(member) || member == myId)

    register(newMembers)
    currentClusterConfiguration.set(newClusterConfiguration)
    logger.info("Cluster Configuration changed to {}", clusterConfiguration)
  }

  def register(newMembers: Set[String]) {
    currentKnownMembers.set(knownMembers ++ newMembers.map(id ⇒ (id, createRemoteMember(id))))
  }

  def happensBefore(index: Index) = clusterConfiguration.index < index

  def isCurrent(index: Index) = index == clusterConfiguration.index

  def contains(member: String) = members.contains(member)

  def myId = localMember.id()

  def bootstrap() = {
    //validate empty log and no snapshot
    transitionTo(SingleClusterConfiguration(Set(myId), 1))
  }

  def createRemoteMember(id: String): RemoteMember = new RemoteMember(rpc, id)

  def isInitialized = clusterConfiguration != EmptyClusterConfiguration
}

