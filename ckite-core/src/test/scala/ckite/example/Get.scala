package ckite.example
import ckite.rpc.ReadCommand

case class Get(key: String) extends ReadCommand[String]