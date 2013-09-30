package the.walrus.ckite.statemachine

import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.Result

trait StateMachine {

  def apply(command: Command): Result
  
}