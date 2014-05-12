package ckite.example
import ckite.rpc.ReadCommand

case class Get[Key](key: Key) extends ReadCommand