package ckite

import ckite.rpc.{ ReadCommand, WriteCommand }

import scala.concurrent.Future

/**
 * A CKite is a member of the cluster. It exchanges messages with its peers to achieve consensus
 * on the submitted write and read commands according to the Raft consensus protocol.
 */
trait CKite {

  /**
   * Starts CKite. It begins the communication with the rest of the cluster.
   */
  def start(): Unit

  /**
   * Stops CKite. It no longer receives or sends messages to the cluster. It can't be started again.
   */
  def stop(): Unit

  /**
   * Consistently replicates and applies a command under Raft consensus rules.
   *
   * @param writeCommand to be applied
   * @tparam T
   * @return a Future with the result of applying the Write to the StateMachine
   */
  def write[T](writeCommand: WriteCommand[T]): Future[T]

  /**
   * Consistent read. It is forwarded and answered by the Leader according to Raft consensus rules.
   *
   * @param readCommand to be forwarded and applied to the Leader StateMachine
   * @tparam T
   * @return a Future with the result of applying the Read to the StateMachine
   */
  def read[T](readCommand: ReadCommand[T]): Future[T]

  /**
   * Consistently adds a new member to the cluster.
   * *
   * @param memberId to be added
   * @return future with true if the memberId could be added to the cluster and false if not
   */
  def addMember(memberId: String): Future[Boolean]

  /**
   * Consistently removes a new member to the cluster.
   * *
   * @param memberId to be removed
   * @return future with true if the memberId could be removed to the cluster and false if not
   */
  def removeMember(memberId: String): Future[Boolean]

}