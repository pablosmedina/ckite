package ckite.stats

class StateInfo

case class LeaderInfo(leaderUptime: String, followers: Map[String, FollowerInfo]) extends StateInfo

case class NonLeaderInfo(following: String) extends StateInfo

case class FollowerInfo(lastHeartbeatACK: String, matchIndex: Int, nextIndex: Int)