package the.walrus.ckite.example
import the.walrus.ckite.rpc.ReadCommand

case class Get[Key](key: Key) extends ReadCommand