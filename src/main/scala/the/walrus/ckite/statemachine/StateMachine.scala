package the.walrus.ckite.statemachine

import the.walrus.ckite.rpc.WriteCommand
import the.walrus.ckite.rpc.ReadCommand
import the.walrus.ckite.rpc.Command

trait StateMachine {

  def apply(command: Command): Any
  
}