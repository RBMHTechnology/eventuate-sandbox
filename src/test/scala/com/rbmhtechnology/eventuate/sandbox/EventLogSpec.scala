package com.rbmhtechnology.eventuate.sandbox

import akka.actor._
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout

import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._

import org.scalatest._

import scala.collection.immutable.Seq

object EventLogSpec {
  val EmitterId1 = "E1"
  val EmitterId2 = "E2"

  val LogId1 = "L1"
  val LogId2 = "L2"
  val LogId3 = "L3"

  class ExcludePayload(payload: String) extends ReplicationFilter {
    override def apply(event: EncodedEvent): Boolean =
      event.decode.payload != payload
  }

  def excludePayload(payload: String): ReplicationFilter =
    new ExcludePayload(payload)
}

class EventLogSpec extends TestKit(ActorSystem("test")) with AsyncWordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import EventLogSpec._
  import EventLog._

  private var log: ActorRef = _

  private val settings =
    new ReplicationSettings(system.settings.config)

  implicit val timeout =
    Timeout(settings.askTimeout)

  override protected def beforeEach(): Unit =
    log = system.actorOf(EventLog.props(LogId1, Map(LogId2 -> excludePayload("y")), excludePayload("z")))

  override protected def afterEach(): Unit =
    system.stop(log)

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "An EventLog" must {
    "process Write and Read" in {
      val emitted = Seq(
        DecodedEvent(EmitterId1, "a"),
        DecodedEvent(EmitterId1, "b"))

      val expected = Seq(
        DecodedEvent(EventMetadata(EmitterId1, LogId1, LogId1, 1L, VectorTime(LogId1 -> 1L)), "a"),
        DecodedEvent(EventMetadata(EmitterId1, LogId1, LogId1, 2L, VectorTime(LogId1 -> 2L)), "b"))

      log.ask(Write(emitted)).map {
        case WriteSuccess(events) => events should be(expected)
      }
      log.ask(Read(1L)).map {
        case ReadSuccess(events) => events should be(expected)
      }
      log.ask(Read(2L)).map {
        case ReadSuccess(events) => events should be(expected.tail)
      }
    }
    "process ReplicationWrite and ReplicationRead" in {
      val replicated = Seq(
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 1L, VectorTime(LogId2 -> 1L)), "a"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 2L, VectorTime(LogId2 -> 2L)), "b"))

      val expected = Seq(
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 1L, VectorTime(LogId2 -> 1L)), "a"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 2L, VectorTime(LogId2 -> 2L)), "b"))

      log.ask(ReplicationWrite(encode(replicated), Map(LogId2 -> 2L))).map {
        case ReplicationWriteSuccess(events, progresses, versionVector) =>
          decode(events) should be(expected)
          progresses should be(Map(LogId2 -> 2L))
          versionVector should be(VectorTime(LogId2 -> 2L))
      }
      log.ask(ReplicationRead(1L, settings.batchSize, LogId2, VectorTime.Zero)).map {
        case ReplicationReadSuccess(events, progress) =>
          decode(events) should be(expected)
      }
      log.ask(ReplicationRead(1L, settings.batchSize, LogId2, VectorTime(LogId2 -> 1L))).map {
        case ReplicationReadSuccess(events, progress) =>
          decode(events) should be(expected.drop(1))
      }
    }
    "apply replication filters to ReplicationRead" in {
      val replicated = Seq(
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 1L, VectorTime(LogId2 -> 1L)), "x"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 2L, VectorTime(LogId2 -> 2L)), "y"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 3L, VectorTime(LogId2 -> 3L)), "z"))

      val expected = Seq(
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 1L, VectorTime(LogId2 -> 1L)), "x"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 2L, VectorTime(LogId2 -> 2L)), "y"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 3L, VectorTime(LogId2 -> 3L)), "z"))

      log.ask(ReplicationWrite(encode(replicated), Map(LogId2 -> 2L))).map {
        case ReplicationWriteSuccess(events, progresses, _) =>
          decode(events) should be(expected)
      }
      log.ask(ReplicationRead(1L, settings.batchSize, LogId2, VectorTime.Zero)).map {
        case ReplicationReadSuccess(events, progress) =>
          decode(events) should be(expected.take(1))
      }
      log.ask(ReplicationRead(1L, settings.batchSize, LogId3, VectorTime.Zero)).map {
        case ReplicationReadSuccess(events, progress) =>
          decode(events) should be(expected.take(2))
      }
    }
    "publish on Write" in {
      val emitted = Seq(
        DecodedEvent(EmitterId1, "a"),
        DecodedEvent(EmitterId1, "b"))

      val probe = TestProbe()

      log ! Subscribe(probe.ref)
      log ? Write(emitted)

      probe.expectMsg(DecodedEvent(EventMetadata(EmitterId1, LogId1, LogId1, 1L, VectorTime(LogId1 -> 1L)), "a"))
      probe.expectMsg(DecodedEvent(EventMetadata(EmitterId1, LogId1, LogId1, 2L, VectorTime(LogId1 -> 2L)), "b"))

      succeed
    }
    "publish on ReplicationWrite" in {
      val replicated = Seq(
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 1L, VectorTime(LogId2 -> 1L)), "a"),
        DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId2, 2L, VectorTime(LogId2 -> 2L)), "b"))

      val probe = TestProbe()

      log ! Subscribe(probe.ref)
      log ? ReplicationWrite(encode(replicated), Map(LogId2 -> 2L))

      probe.expectMsg(DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 1L, VectorTime(LogId2 -> 1L)), "a"))
      probe.expectMsg(DecodedEvent(EventMetadata(EmitterId2, LogId2, LogId1, 2L, VectorTime(LogId2 -> 2L)), "b"))

      succeed
    }
  }
}
