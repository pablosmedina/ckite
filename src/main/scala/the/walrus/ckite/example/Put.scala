package the.walrus.ckite.example

import the.walrus.ckite.rpc.WriteCommand

case class Put[Key,Value](key: Key, value: Value) extends WriteCommand