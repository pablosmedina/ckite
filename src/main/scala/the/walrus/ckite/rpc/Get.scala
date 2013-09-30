package the.walrus.ckite.rpc

case class Get[Key](key: Key) extends Command