package ckite.rlog

import ckite.rpc.LogEntry.Term

trait Storage {
  def log(): Log

  def saveVote(vote: Vote)

  def retrieveLatestVote(): Option[Vote]

  def saveSnapshot(snapshot: Snapshot)

  def retrieveLatestSnapshot(): Option[Snapshot]
}

case class Vote(term: Term, member: String)
