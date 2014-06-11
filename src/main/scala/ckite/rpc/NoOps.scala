package ckite.rpc

case class NoOp extends WriteCommand[Unit]

case object Void

case class CompactedEntry() extends ReadCommand[Unit]