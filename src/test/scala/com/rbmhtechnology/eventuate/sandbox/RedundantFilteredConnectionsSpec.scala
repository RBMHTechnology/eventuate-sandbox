package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Subscribe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Write
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.WordSpec

import scala.concurrent.duration.DurationInt

object RedundantFilteredConnectionsSpec {
  val EmitterIdA = "EM-A"
  val EmitterIdB1 = "EM-B1"
  val EmitterIdB2 = "EM-B2"
  val EndpointIdA = "EP-A"
  val EndpointIdB1 = "EP-B1"
  val EndpointIdB2 = "EP-B2"

  val LogName = "L"

  case object InternalEvent
  case object ExternalEvent

  def replicationFilter(implicit system: ActorSystem): ReplicationFilter = new ReplicationFilter {
    override def apply(event: EncodedEvent) =
      EventPayloadSerializer.decode(event).get.payload.isInstanceOf[ExternalEvent.type]
  }

  def payloadEquals(payload: AnyRef): PartialFunction[Any, Any] = {
    case DecodedEvent(_, actual) if actual == payload => actual
  }

  def bidiConnect(endpoint1: ReplicationEndpoint, endpoint2: ReplicationEndpoint): Unit = {
    endpoint1.connect(endpoint2)
    endpoint2.connect(endpoint1)
  }

  def expectPayloads(probe: TestProbe, payloads: AnyRef*): Unit =
    payloads.foreach { payload =>
      probe.expectMsgPF(hint = payload.toString)(payloadEquals(payload))
    }
}

class RedundantFilteredConnectionsSpec extends WordSpec with BeforeAndAfterEach {

  import RedundantFilteredConnectionsSpec._

  private var endpointA: ReplicationEndpoint = _
  private var endpointB1: ReplicationEndpoint = _
  private var endpointB2: ReplicationEndpoint = _
  private var probeA: TestProbe = _
  private var probeB1: TestProbe = _
  private var probeB2: TestProbe = _
  private var logA: ActorRef = _
  private var logB1: ActorRef = _
  private var logB2: ActorRef = _

  override protected def beforeEach(): Unit = {
    endpointA = new ReplicationEndpoint(EndpointIdA, Set(LogName), Map())
    endpointB1 = new ReplicationEndpoint(EndpointIdB1, Set(LogName), Map())
    endpointB2 = new ReplicationEndpoint(EndpointIdB2, Set(LogName), Map())

    probeA = TestProbe()(endpointA.system)
    probeB1 = TestProbe()(endpointB1.system)
    probeB2 = TestProbe()(endpointB2.system)

    logA = endpointA.eventLogs(LogName)
    logB1 = endpointB1.eventLogs(LogName)
    logB2 = endpointB2.eventLogs(LogName)
    logA ! Subscribe(probeA.ref)
    logB1 ! Subscribe(probeB1.ref)
    logB2 ! Subscribe(probeB2.ref)
    //     A
    //   /   \ (RFCs)
    // B1 --- B2
    // connection B1 - B2 initially interrupted
    endpointA.addRedundantFilterConfig(EndpointIdB1, LogName, Set(EndpointIdB1, EndpointIdB2))
    endpointA.addRedundantFilterConfig(EndpointIdB2, LogName, Set(EndpointIdB1, EndpointIdB2))
    endpointB1.addTargetFilter(EndpointIdA, LogName, replicationFilter(endpointB1.system))
    endpointB2.addTargetFilter(EndpointIdA, LogName, replicationFilter(endpointB2.system))
    bidiConnect(endpointA, endpointB1)
    bidiConnect(endpointA, endpointB2)
  }

  override protected def afterEach(): Unit = {
    endpointA.terminate()
    endpointB1.terminate()
    endpointB2.terminate()
  }

  "ReplicationEndpoint" must {
    "stop replication over redundant filtered connections on event from replicated application" in {
      logA ! Write(List(DecodedEvent(EmitterIdA, ExternalEvent)))
      expectPayloads(probeA, ExternalEvent)
      expectPayloads(probeB1, ExternalEvent)
      expectPayloads(probeB2, ExternalEvent)

      logB1 ! Write(List(DecodedEvent(EmitterIdB1, InternalEvent), DecodedEvent(EmitterIdB1, ExternalEvent)))
      expectPayloads(probeA, ExternalEvent)
      logA ! Write(List(DecodedEvent(EmitterIdA, ExternalEvent)))
      probeB2.expectNoMsg(500.millis)

      bidiConnect(endpointB2, endpointB1)

      expectPayloads(probeB2, InternalEvent, ExternalEvent, ExternalEvent)
    }
  }
}
