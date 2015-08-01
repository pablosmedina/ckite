package ckite.statemachine

import java.nio.ByteBuffer
import ckite.rpc.Command
import ckite.rpc.ReadCommand
import ckite.rpc.WriteCommand

trait StateMachine {

  /**
   * Called when consensus has been reached on a WriteCommand.
   * Along with the WriteCommand an index is provided to allow
   * persistent StateMachines to save atomically both the WriteCommand's
   * updates and the index. CKite will ask the lastAppliedIndex
   * when deciding which WriteCommands can be replayed during startups.
   */
  def applyWrite: PartialFunction[(Long, WriteCommand[_]), Any]

  /**
   * The last applied index in the StateMachine. It is called
   */
  def lastAppliedIndex: Long

  /**
   * Called when readonly commands are requested.
   */
  def applyRead: PartialFunction[ReadCommand[_], Any]

  /**
   * Restore the StateMachine state from a Snapshot
   */
  def deserialize(byteBuffer: ByteBuffer)

  /**
   * Captures the StateMachine state as a Snapshot
   */
  def serialize(): ByteBuffer

}