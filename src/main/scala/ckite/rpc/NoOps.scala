package ckite.rpc

case class NoOp extends WriteCommand

case object Void

case class CompactedEntry() extends ReadCommand