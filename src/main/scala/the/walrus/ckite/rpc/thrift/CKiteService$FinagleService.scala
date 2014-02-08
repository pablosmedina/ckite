/**
 * Generated by Scrooge
 *   version: ?
 *   rev: ?
 *   built at: ?
 */
package the.walrus.ckite.rpc.thrift

import com.twitter.finagle.{Service => FinagleService}
import com.twitter.scrooge.ThriftStruct
import com.twitter.util.Future
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.thrift.protocol._
import org.apache.thrift.TApplicationException
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import scala.collection.mutable.{
  ArrayBuffer => mutable$ArrayBuffer, HashMap => mutable$HashMap}
import scala.collection.{Map, Set}


@javax.annotation.Generated(value = Array("com.twitter.scrooge.Compiler"), date = "2014-02-08T02:35:28.747-0300")
class CKiteService$FinagleService(
  iface: CKiteService[Future],
  protocolFactory: TProtocolFactory
) extends FinagleService[Array[Byte], Array[Byte]] {
  import CKiteService._

  protected val functionMap = new mutable$HashMap[String, (TProtocol, Int) => Future[Array[Byte]]]()

  protected def addFunction(name: String, f: (TProtocol, Int) => Future[Array[Byte]]) {
    functionMap(name) = f
  }

  protected def exception(name: String, seqid: Int, code: Int, message: String): Future[Array[Byte]] = {
    try {
      val x = new TApplicationException(code, message)
      val memoryBuffer = new TMemoryBuffer(512)
      val oprot = protocolFactory.getProtocol(memoryBuffer)

      oprot.writeMessageBegin(new TMessage(name, TMessageType.EXCEPTION, seqid))
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.getTransport().flush()
      Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()))
    } catch {
      case e: Exception => Future.exception(e)
    }
  }

  protected def reply(name: String, seqid: Int, result: ThriftStruct): Future[Array[Byte]] = {
    try {
      val memoryBuffer = new TMemoryBuffer(512)
      val oprot = protocolFactory.getProtocol(memoryBuffer)

      oprot.writeMessageBegin(new TMessage(name, TMessageType.REPLY, seqid))
      result.write(oprot)
      oprot.writeMessageEnd()

      Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()))
    } catch {
      case e: Exception => Future.exception(e)
    }
  }

  final def apply(request: Array[Byte]): Future[Array[Byte]] = {
    val inputTransport = new TMemoryInputTransport(request)
    val iprot = protocolFactory.getProtocol(inputTransport)

    try {
      val msg = iprot.readMessageBegin()
      functionMap.get(msg.name) map { _.apply(iprot, msg.seqid) } getOrElse {
        TProtocolUtil.skip(iprot, TType.STRUCT)
        exception(msg.name, msg.seqid, TApplicationException.UNKNOWN_METHOD,
          "Invalid method name: '" + msg.name + "'")
      }
    } catch {
      case e: Exception => Future.exception(e)
    }
  }

  // ---- end boilerplate.

  addFunction("sendRequestVote", { (iprot: TProtocol, seqid: Int) =>
    try {
      val args = sendRequestVote$args.decode(iprot)
      iprot.readMessageEnd()
      (try {
        iface.sendRequestVote(args.requestVote)
      } catch {
        case e: Exception => Future.exception(e)
      }) flatMap { value: RequestVoteResponseST =>
        reply("sendRequestVote", seqid, sendRequestVote$result(success = Some(value)))
      } rescue {
        case e => Future.exception(e)
      }
    } catch {
      case e: TProtocolException => {
        iprot.readMessageEnd()
        exception("sendRequestVote", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage)
      }
      case e: Exception => Future.exception(e)
    }
  })
  addFunction("sendAppendEntries", { (iprot: TProtocol, seqid: Int) =>
    try {
      val args = sendAppendEntries$args.decode(iprot)
      iprot.readMessageEnd()
      (try {
        iface.sendAppendEntries(args.appendEntries)
      } catch {
        case e: Exception => Future.exception(e)
      }) flatMap { value: AppendEntriesResponseST =>
        reply("sendAppendEntries", seqid, sendAppendEntries$result(success = Some(value)))
      } rescue {
        case e => Future.exception(e)
      }
    } catch {
      case e: TProtocolException => {
        iprot.readMessageEnd()
        exception("sendAppendEntries", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage)
      }
      case e: Exception => Future.exception(e)
    }
  })
  addFunction("forwardCommand", { (iprot: TProtocol, seqid: Int) =>
    try {
      val args = forwardCommand$args.decode(iprot)
      iprot.readMessageEnd()
      (try {
        iface.forwardCommand(args.command)
      } catch {
        case e: Exception => Future.exception(e)
      }) flatMap { value: ByteBuffer =>
        reply("forwardCommand", seqid, forwardCommand$result(success = Some(value)))
      } rescue {
        case e => Future.exception(e)
      }
    } catch {
      case e: TProtocolException => {
        iprot.readMessageEnd()
        exception("forwardCommand", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage)
      }
      case e: Exception => Future.exception(e)
    }
  })
  addFunction("installSnapshot", { (iprot: TProtocol, seqid: Int) =>
    try {
      val args = installSnapshot$args.decode(iprot)
      iprot.readMessageEnd()
      (try {
        iface.installSnapshot(args.installSnapshot)
      } catch {
        case e: Exception => Future.exception(e)
      }) flatMap { value: Boolean =>
        reply("installSnapshot", seqid, installSnapshot$result(success = Some(value)))
      } rescue {
        case e => Future.exception(e)
      }
    } catch {
      case e: TProtocolException => {
        iprot.readMessageEnd()
        exception("installSnapshot", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage)
      }
      case e: Exception => Future.exception(e)
    }
  })
  addFunction("join", { (iprot: TProtocol, seqid: Int) =>
    try {
      val args = join$args.decode(iprot)
      iprot.readMessageEnd()
      (try {
        iface.join(args.memberId)
      } catch {
        case e: Exception => Future.exception(e)
      }) flatMap { value: JoinResponseST =>
        reply("join", seqid, join$result(success = Some(value)))
      } rescue {
        case e => Future.exception(e)
      }
    } catch {
      case e: TProtocolException => {
        iprot.readMessageEnd()
        exception("join", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage)
      }
      case e: Exception => Future.exception(e)
    }
  })
  addFunction("getMembers", { (iprot: TProtocol, seqid: Int) =>
    try {
      val args = getMembers$args.decode(iprot)
      iprot.readMessageEnd()
      (try {
        iface.getMembers()
      } catch {
        case e: Exception => Future.exception(e)
      }) flatMap { value: GetMembersResponseST =>
        reply("getMembers", seqid, getMembers$result(success = Some(value)))
      } rescue {
        case e => Future.exception(e)
      }
    } catch {
      case e: TProtocolException => {
        iprot.readMessageEnd()
        exception("getMembers", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage)
      }
      case e: Exception => Future.exception(e)
    }
  })
}