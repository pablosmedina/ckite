package ckite.statemachine

import java.nio.ByteBuffer

import ckite.rpc.{ ReadCommand, WriteCommand }

trait StateMachine {

  /**
   * Called when consensus has been reached on a WriteCommand.
   * Along with the WriteCommand an index is provided to allow
   * persistent StateMachines to save atomically both the WriteCommand's
   * updates and the index.
   * CKite will ask the lastAppliedIndex when deciding which WriteCommands can be replayed during startup.
   *
   * Memory consistency effects: Since all the operations on the StateMachine are done by
   * a single thread then every read, write or snapshot operation happens-before the subsequent
   * read, write or snapshot operation.
   */
  def applyWrite: PartialFunction[(Long, WriteCommand[_]), Any]

  /**
   * The last applied index in the StateMachine.
   */
  def getLastAppliedIndex: Long

  /**
   * Called when readonly commands are requested.
   */
  def applyRead: PartialFunction[ReadCommand[_], Any]

  /**
   * Restore the StateMachine state from a Snapshot
   */
  def restoreSnapshot(byteBuffer: ByteBuffer)

  /**
   * Captures the StateMachine state as a Snapshot
   */
  def takeSnapshot(): ByteBuffer

}