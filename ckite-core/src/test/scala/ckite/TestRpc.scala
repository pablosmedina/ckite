package ckite

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import ckite.rpc._

import scala.concurrent.Future
import scala.util.Try

object TestRpc extends Rpc {

  val servers = new ConcurrentHashMap[String, TestServer]()

  def server(binding: String): Raft = {
    val server = servers.get(binding)
    if (server == null || server.isStopped() || server.isBlocked) {
      throw new IOException("Connection refused")
    }
    server.cluster
  }

  def blockTraffic(binding: String) = {
    servers.get(binding).block()
  }

  def unblockTraffic(binding: String) = {
    servers.get(binding).unblock()
  }

  def isBlocked(binding: String) = servers.get(binding).isBlocked

  override def createServer(service: RpcService): RpcServer = {
    val testServer: TestServer = new TestServer(service.asInstanceOf[Raft])
    servers.put(service.asInstanceOf[Raft].membership.myId, testServer)
    testServer
  }

  override def createClient(binding: String): RpcClient = new TestClient(binding)
}

class TestServer(val cluster: Raft) extends RpcServer {
  val stopped = new AtomicBoolean()
  val blocked = new AtomicBoolean()

  override def start(): Unit = {
    stopped.set(false)
  }

  override def stop(): Unit = {

    stopped.set(true)
  }

  def block() = {
    blocked.set(true)
  }

  def unblock() = {
    blocked.set(false)
  }

  def isStopped() = stopped.get()

  def isBlocked = blocked.get()

}

class TestClient(binding: String) extends RpcClient {
  override def send(request: RequestVote): Future[RequestVoteResponse] = ioTry {
    if (TestRpc.isBlocked(request.memberId)) {
      throw new IOException("Connection refused")
    }
    TestRpc.server(binding).onRequestVoteReceived(request)
  }

  override def send(appendEntries: AppendEntries): Future[AppendEntriesResponse] = ioTry {
    if (TestRpc.isBlocked(appendEntries.leaderId)) {
      throw new IOException("Connection refused")
    }
    TestRpc.server(binding).onAppendEntriesReceived(appendEntries)
  }

  override def send(installSnapshot: InstallSnapshot): Future[InstallSnapshotResponse] = ioTry {
    if (TestRpc.isBlocked(installSnapshot.leaderId)) {
      throw new IOException("Connection refused")
    }
    TestRpc.server(binding).onInstallSnapshotReceived(installSnapshot)
  }

  override def send[T](command: Command): Future[T] = ioTry {
    TestRpc.server(binding).onCommandReceived(command)
  }

  override def send(joinMember: JoinMember): Future[JoinMemberResponse] = ioTry {
    if (TestRpc.isBlocked(joinMember.memberId)) {
      throw new IOException("Connection refused")
    }
    TestRpc.server(binding).onMemberJoinReceived(joinMember.memberId)
  }

  def ioTry[T](block: ⇒ Future[T]): Future[T] = {
    Try {
      block
    }.recover {
      case e: IOException ⇒ Future.failed(e)
    }.get
  }
}
