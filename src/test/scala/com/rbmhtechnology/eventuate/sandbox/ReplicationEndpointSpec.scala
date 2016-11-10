package com.rbmhtechnology.eventuate.sandbox

import akka.actor._
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout

import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint._

import org.scalatest._

object ReplicationEndpointSpec {
  val EmitterId1 = "EM1"
  val EmitterId2 = "EM2"

  val EndpointId1 = "EP1"
  val EndpointId2 = "EP2"

  val LogName = "L"
}

class ReplicationEndpointSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import ReplicationEndpointSpec._

  private var endpoint1: ReplicationEndpoint = _
  private var endpoint2: ReplicationEndpoint = _

  private val settings =
    new ReplicationSettings(system.settings.config)

  implicit val timeout =
    Timeout(settings.askTimeout)

  override protected def beforeEach(): Unit = {
    endpoint1 = new ReplicationEndpoint(EndpointId1, Set(LogName), Map(), Map())
    endpoint2 = new ReplicationEndpoint(EndpointId2, Set(LogName), Map(), Map())
  }

  override protected def afterEach(): Unit = {
    endpoint1.terminate()
    endpoint2.terminate()
  }

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "Replication endpoints" must {
    "replicate events" in {
      val probe1 = TestProbe()
      val probe2 = TestProbe()

      val log1 = endpoint1.eventLogs(LogName)
      val log2 = endpoint2.eventLogs(LogName)

      val log1Id = logId(endpoint1.id, LogName)
      val log2Id = logId(endpoint2.id, LogName)

      log1 ! Subscribe(probe1.ref)
      log2 ! Subscribe(probe2.ref)

      endpoint1.connect(endpoint2)
      endpoint2.connect(endpoint1)

      log1 ? Write(List(DecodedEvent(EmitterId1, "a")))
      log1 ? Write(List(DecodedEvent(EmitterId1, "b")))

      probe1.expectMsg(DecodedEvent(EventMetadata(EmitterId1, log1Id, log1Id, 1L, VectorTime(log1Id -> 1L)), "a"))
      probe1.expectMsg(DecodedEvent(EventMetadata(EmitterId1, log1Id, log1Id, 2L, VectorTime(log1Id -> 2L)), "b"))
      probe2.expectMsg(DecodedEvent(EventMetadata(EmitterId1, log1Id, log2Id, 1L, VectorTime(log1Id -> 1L)), "a"))
      probe2.expectMsg(DecodedEvent(EventMetadata(EmitterId1, log1Id, log2Id, 2L, VectorTime(log1Id -> 2L)), "b"))

      log2 ? Write(List(DecodedEvent(EmitterId2, "x")))
      log2 ? Write(List(DecodedEvent(EmitterId2, "y")))

      probe2.expectMsg(DecodedEvent(EventMetadata(EmitterId2, log2Id, log2Id, 3L, VectorTime(log2Id -> 3L)), "x"))
      probe2.expectMsg(DecodedEvent(EventMetadata(EmitterId2, log2Id, log2Id, 4L, VectorTime(log2Id -> 4L)), "y"))
      probe1.expectMsg(DecodedEvent(EventMetadata(EmitterId2, log2Id, log1Id, 3L, VectorTime(log2Id -> 3L)), "x"))
      probe1.expectMsg(DecodedEvent(EventMetadata(EmitterId2, log2Id, log1Id, 4L, VectorTime(log2Id -> 4L)), "y"))
    }
  }
}
