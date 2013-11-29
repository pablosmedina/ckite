package the.walrus.ckite.states

import org.slf4j.LoggerFactory
import the.walrus.ckite.rpc.RequestVote
import the.walrus.ckite.Cluster
import the.walrus.ckite.rpc.Command
import the.walrus.ckite.rpc.RequestVoteResponse
import the.walrus.ckite.rpc.AppendEntriesResponse
import the.walrus.ckite.rpc.AppendEntries

/** 	•! Increment currentTerm, vote for self
 * •! Reset election timeout
 * •! Send RequestVote RPCs to all other servers, wait for either:
 * •! Votes received from majority of servers: become leader
 * •! AppendEntries RPC received from new leader: step
 * down
 * •! Election timeout elapses without election resolution:
 * increment term, start new election
 * •! Discover higher term: step down (§5.1)
 */
case object Candidate extends State {

  override def begin(term: Int)(implicit cluster: Cluster) = {
    val inTerm = cluster.local.incrementTerm
    cluster.setNoLeader()
    cluster.local.voteForMyself()
    LOG.info(s"Start election")
    val votes = cluster.collectVotes()
    LOG.debug(s"Got $votes votes in a majority of ${cluster.majority}")
    if (votes >= cluster.majority) {
      cluster.local becomeLeader inTerm
    } else {
      LOG.info(s"Not enough votes to be a Leader")
      cluster.local becomeFollower inTerm
    }
  }

  override def onAppendEntriesReceived(appendEntries: AppendEntries)(implicit cluster: Cluster): AppendEntriesResponse = {
    if (appendEntries.term < cluster.local.term) {
      AppendEntriesResponse(cluster.local.term, false)
    }
    else {
      stepDown(Some(appendEntries.leaderId), appendEntries.term)
      cluster.local.onAppendEntriesReceived(appendEntries) 
    }
  }

  override def onRequestVoteReceived(requestVote: RequestVote)(implicit cluster: Cluster): RequestVoteResponse = {
    if (requestVote.term <= cluster.local.term) {
      RequestVoteResponse(cluster.local.term, false)
    } else {
      stepDown(None, requestVote.term)
      cluster.local.onMemberRequestingVoteReceived(requestVote)
    }
  }
  
  override def onCommandReceived(command: Command)(implicit cluster: Cluster) = {
    cluster.forwardToLeader(command)
  }

}