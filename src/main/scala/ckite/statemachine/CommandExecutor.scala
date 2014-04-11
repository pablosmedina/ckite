package ckite.statemachine

import ckite.rpc.Command
import com.esotericsoftware.reflectasm.MethodAccess

class CommandExecutor(stateMachine: StateMachine) {
  val methodAccess = MethodAccess.get(stateMachine.getClass())
  val commandMethodsMap = buildCommandMethodsMap

  private def buildCommandMethodsMap = {
    val res = methodAccess.getMethodNames().filter(methodName => methodName.startsWith("on"))
      .map { methodName => (methodName.substring(2), methodAccess.getIndex(methodName)) }
    res.toMap
  }

  def apply(command: Command): Any = {
    if (command.isInstanceOf[Product]) {
      val fields = command.asInstanceOf[Product].productIterator.toArray.asInstanceOf[Array[Object]]
      val commandName = command.getClass().getSimpleName()
      commandMethodsMap.get(commandName) map { methodIndex =>
        methodAccess.invoke(stateMachine, methodIndex, fields: _*)
      } get
    } else throw new UnsupportedOperationException("No command handler in StateMachine")
  }

}