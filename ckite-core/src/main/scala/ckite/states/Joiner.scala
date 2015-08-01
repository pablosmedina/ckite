package ckite.states

import ckite._
import ckite.rpc.LogEntry.Term

object Joiner {
  def apply(consensus: Consensus, membership: Membership, log: RLog, term: Term, configuration: Configuration): Follower = {
    new Follower(consensus, membership, log, term, LeaderAnnouncer(membership, configuration), None) with NoElectionTimeout {
      override val toString = s"Joiner[$term]"
    }
  }
}
