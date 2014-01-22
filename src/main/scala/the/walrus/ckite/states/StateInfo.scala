package the.walrus.ckite.states

class StateInfo

case class LeaderInfo(leaderUptime: String, lastHeartbeatsACKs: Map[String, String]) extends StateInfo

case class NonLeaderInfo(following: String) extends StateInfo