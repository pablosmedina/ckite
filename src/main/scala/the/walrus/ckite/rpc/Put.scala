package the.walrus.ckite.rpc

case class Put[Key,Value](key: Key, value: Value) extends Command