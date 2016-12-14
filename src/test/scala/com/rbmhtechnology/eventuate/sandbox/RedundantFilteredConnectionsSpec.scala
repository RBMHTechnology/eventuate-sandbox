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

  case class InternalEvent(s: String)
  case class ExternalEvent(s: String)

  def replicationFilter(implicit system: ActorSystem): ReplicationFilter = new ReplicationFilter {
    override def apply(event: EncodedEvent) =
      EventPayloadSerializer.decode(event).get.payload.isInstanceOf[ExternalEvent]
  }

  def payloadEquals(payload: AnyRef): PartialFunction[Any, Any] = {
    case DecodedEvent(_, actual) if actual == payload => actual
  }

  def bidiConnect(location1: Location, location2: Location): Unit = {
    location1.endpoint.connect(location2.endpoint)
    location2.endpoint.connect(location1.endpoint)
  }

  def disconnect(location1: Location, location2: Location): Unit = {
    location1.endpoint.disconnect(location2.endpoint.id)
    location2.endpoint.disconnect(location1.endpoint.id)
  }

  def bidiConnect(location1: Location, location2: Location, outboundFilter1: ReplicationFilter = NoFilter, outboundFilter2: ReplicationFilter = NoFilter, rfcEndpoints1: Set[Location]= Set.empty, rfcEndpoints2: Set[Location]= Set.empty): Unit = {
    if(rfcEndpoints1.nonEmpty) location1.endpoint.addRedundantFilterConfig(location2.endpoint.id, LogName, rfcEndpoints1.map(_.endpoint.id))
    if(rfcEndpoints2.nonEmpty) location2.endpoint.addRedundantFilterConfig(location1.endpoint.id, LogName, rfcEndpoints2.map(_.endpoint.id))
    if(outboundFilter1 ne NoFilter) location1.endpoint.addTargetFilter(location2.endpoint.id, LogName, outboundFilter1)
    if(outboundFilter2 ne NoFilter) location2.endpoint.addTargetFilter(location1.endpoint.id, LogName, outboundFilter2)
    bidiConnect(location1, location2)
  }

  def expectPayloads(payloads: Seq[AnyRef], locs: Location*) =
    locs.foreach(_.expectPayloads(payloads))

  def event(payload: AnyRef): DecodedEvent = DecodedEvent("emitter-id", payload)

  class Location(id: String) {
    var eventCnt = 0
    val endpoint = new ReplicationEndpoint(s"EP-$id", Set(LogName))
    val probe = TestProbe(s"P-$id")(endpoint.system)
    val log = endpoint.eventLogs(LogName)
    log ! Subscribe(probe.ref)

    def emit(makePayloads: Function1[String, AnyRef]*): Seq[AnyRef] = {
      val payloads = makePayloads.toList.map {
        eventCnt += 1
        _(s"$id.$eventCnt")
      }
      log ! Write(payloads.map(DecodedEvent(s"EM-$id", _)))
      payloads
    }

    def emitN(makePayload: String => AnyRef, n: Int = 1): Seq[AnyRef] =
      emit(List.fill(n)(makePayload): _*)

    def expectPayloads(payloads: Seq[AnyRef]): Unit =
      payloads.foreach { payload =>
        probe.expectMsgPF(hint = s"${probe.ref} expects $payload")(payloadEquals(payload))
      }

    def expectNoMsg(): Unit =
      probe.expectNoMsg(200.millis)

    val filter = replicationFilter(endpoint.system)
  }
}

class RedundantFilteredConnectionsSpec extends WordSpec with BeforeAndAfterEach {

  import RedundantFilteredConnectionsSpec._

  private var systems: List[ActorSystem] = Nil

  def newLocations(ids: String*): List[Location] = {
    val res = ids.toList.map(i => new Location(i))
    systems = res.map(_.endpoint.system)
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
      val a :: b1 :: b2 :: Nil = newLocations("A", "B1", "B2")
      val redundantConnectionsA = Set(b1, b2)
      bidiConnect(a, b1, outboundFilter2 = b1.filter, rfcEndpoints1 = redundantConnectionsA)
      bidiConnect(a, b2, outboundFilter2 = b2.filter, rfcEndpoints1 = redundantConnectionsA)

      expectPayloads(a.emit(ExternalEvent), a, b1, b2)

      val fromB1 = b1.emit(InternalEvent, ExternalEvent)
      expectPayloads(fromB1.filter(_.isInstanceOf[ExternalEvent]), a)

      val fromA = a.emit(ExternalEvent)
      b2.expectNoMsg()

      bidiConnect(b2, b1)

      expectPayloads(fromB1 ++ fromA, b2)
    }
    "stop replication over redundant filtered from replicated application A1, A2 to replicated application B1, B2 and vice versa" in {
      // A1 - A2
      // | RFC |
      // B1 - B2
      // A1, A2 initially interrupted
      val a1 :: a2 :: b1 :: b2 :: Nil = newLocations("A1", "A2", "B1", "B2")
      val redundantConnectionsA = Set(b1, b2)
      val redundantConnectionsB = Set(a1, a2)
      bidiConnect(
        location1 = a1, outboundFilter1 = a1.filter, rfcEndpoints1 = redundantConnectionsA,
        location2 = b1, outboundFilter2 = b1.filter, rfcEndpoints2 = redundantConnectionsB)
      bidiConnect(
        location1 = a2, outboundFilter1 = a2.filter, rfcEndpoints1 = redundantConnectionsA,
        location2 = b2, outboundFilter2 = b2.filter, rfcEndpoints2 = redundantConnectionsB)
      bidiConnect(b1, b2)

      expectPayloads(b1.emit(ExternalEvent), b1, a1, b2, a2)
      expectPayloads(b2.emit(ExternalEvent), b2, a2, b1, a1)

      val fromA1 = a1.emit(ExternalEvent)
      expectPayloads(fromA1, a1, b1, b2)
      val fromB2 = b2.emit(ExternalEvent)
      expectPayloads(fromB2, b2, b1, a1)
      a2.expectNoMsg()

      disconnect(b1, b2)
      bidiConnect(a1, a2)
      expectPayloads(fromA1 ++ fromB2, a2)

      val fromB2_2 = b2.emit(ExternalEvent)
      expectPayloads(fromB2_2, b2, a2, a1)
      b1.expectNoMsg()

      bidiConnect(b1, b2)
      expectPayloads(fromB2_2, b1)
    }
  }
}
