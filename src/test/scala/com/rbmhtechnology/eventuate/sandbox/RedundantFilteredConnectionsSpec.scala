package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Subscribe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Write
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.WordSpec

import scala.concurrent.duration.DurationInt
import scala.collection.immutable.Seq

object RedundantFilteredConnectionsSpec {
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

  def disconnect(endpoint1: ReplicationEndpoint, endpoint2: ReplicationEndpoint): Unit = {
    endpoint1.disconnect(endpoint2.id)
    endpoint2.disconnect(endpoint1.id)
  }

  def bidiConnect(endpoint1: ReplicationEndpoint, endpoint2: ReplicationEndpoint, outboundFilter1: ReplicationFilter = NoFilter, outboundFilter2: ReplicationFilter = NoFilter, rfcEndpoints1: Set[ReplicationEndpoint]= Set.empty, rfcEndpoints2: Set[ReplicationEndpoint]= Set.empty): Unit = {
    if(rfcEndpoints1.nonEmpty) endpoint1.addRedundantFilterConfig(endpoint2.id, LogName, rfcEndpoints1.map(_.id))
    if(rfcEndpoints2.nonEmpty) endpoint2.addRedundantFilterConfig(endpoint1.id, LogName, rfcEndpoints2.map(_.id))
    if(outboundFilter1 ne NoFilter) endpoint1.addTargetFilter(endpoint2.id, LogName, outboundFilter1)
    if(outboundFilter2 ne NoFilter) endpoint2.addTargetFilter(endpoint1.id, LogName, outboundFilter2)
    endpoint1.connect(endpoint2)
    endpoint2.connect(endpoint1)
  }

  def expectPayloads(probe: TestProbe, payloads: AnyRef*): Unit =
    payloads.foreach { payload =>
      probe.expectMsgPF(hint = s"${probe.ref} expects $payload")(payloadEquals(payload))
    }

  def expectPayloads(probes: Seq[TestProbe], payloads: AnyRef*): Unit =
    probes.foreach(expectPayloads(_, payloads: _*))

  def event(payload: AnyRef): DecodedEvent = DecodedEvent("emitter-id", payload)
}

class RedundantFilteredConnectionsSpec extends WordSpec with BeforeAndAfterEach {

  import RedundantFilteredConnectionsSpec._

  private var systems: List[ActorSystem] = Nil

  def newLocations(n: Int): List[(ReplicationEndpoint, TestProbe, ActorRef)] = {
    val res = (1 to n).toList.map { i =>
      val endpoint = new ReplicationEndpoint(s"EP-$i", Set(LogName))
      val probe = TestProbe(s"P-$i")(endpoint.system)
      val log = endpoint.eventLogs(LogName)
      log ! Subscribe(probe.ref)
      (endpoint, probe, log)
    }
    systems = res.map(_._1.system)
    res
  }

  override protected def afterEach(): Unit =
    systems.foreach(_.terminate())

  "ReplicationEndpoint" must {
    "stop replication over redundant filtered from A to replicated application B1, B2" in {
      //    A
      //   / \ (RFC)
      // B1 - B2
      // B1, B2 initially interrupted
      val (a, probeA, logA) :: (b1, probeB1, logB1) :: (b2, probeB2, logB2) :: Nil = newLocations(3)
      val redundantConnectionsA = Set(b1, b2)
      bidiConnect(a, b1, outboundFilter2 = replicationFilter(b1.system), rfcEndpoints1 = redundantConnectionsA)
      bidiConnect(a, b2, outboundFilter2 = replicationFilter(b2.system), rfcEndpoints1 = redundantConnectionsA)

      logA ! Write(List(event(ExternalEvent)))
      expectPayloads(List(probeA, probeB1, probeB2), ExternalEvent)

      logB1 ! Write(List(event(InternalEvent), event(ExternalEvent)))
      expectPayloads(probeA, ExternalEvent)
      logA ! Write(List(event(ExternalEvent)))
      probeB2.expectNoMsg(200.millis)

      bidiConnect(b2, b1)

      expectPayloads(probeB2, InternalEvent, ExternalEvent, ExternalEvent)
    }
    "stop replication over redundant filtered from replicated application A1, A2 to replicated application B1, B2 and vice versa" in {
      // A1 - A2
      // | RFC |
      // B1 - B2
      // A1, A2 initially interrupted
      val (a1, probeA1, logA1) :: (a2, probeA2, logA2) :: (b1, probeB1, logB1) :: (b2, probeB2, logB2) :: Nil = newLocations(4)
      val redundantConnectionsA = Set(b1, b2)
      val redundantConnectionsB = Set(a1, a2)
      bidiConnect(
        endpoint1 = a1, outboundFilter1 = replicationFilter(a1.system), rfcEndpoints1 = redundantConnectionsA,
        endpoint2 = b1, outboundFilter2 = replicationFilter(b1.system), rfcEndpoints2 = redundantConnectionsB)
      bidiConnect(
        endpoint1 = a2, outboundFilter1 = replicationFilter(a2.system), rfcEndpoints1 = redundantConnectionsA,
        endpoint2 = b2, outboundFilter2 = replicationFilter(b2.system), rfcEndpoints2 = redundantConnectionsB)
      bidiConnect(b1, b2)

      logB1 ! Write(List(event(ExternalEvent)))
      expectPayloads(List(probeB1, probeA1, probeB2, probeA2), ExternalEvent)
      logB2 ! Write(List(event(ExternalEvent)))
      expectPayloads(List(probeB2, probeA2, probeB1, probeA1), ExternalEvent)

      logA1 ! Write(List(event(ExternalEvent)))
      expectPayloads(List(probeA1, probeB1, probeB2), ExternalEvent)
      logB2 ! Write(List(event(ExternalEvent)))
      expectPayloads(List(probeB2, probeB1, probeA1), ExternalEvent)
      probeA2.expectNoMsg(200.millis)

      disconnect(b1, b2)
      bidiConnect(a1, a2)
      expectPayloads(probeA2, ExternalEvent, ExternalEvent)

      logB2 ! Write(List(event(ExternalEvent)))
      expectPayloads(List(probeB2, probeA2, probeA1), ExternalEvent)
      probeB1.expectNoMsg(200.millis)

      bidiConnect(b1, b2)
      expectPayloads(probeB1, ExternalEvent)
    }
  }
}
