package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Read
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.ReadSuccess
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Subscribe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Write
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object Location {
  private val settings =
    new ReplicationSettings(ConfigFactory.load())

  implicit val timeout =
    Timeout(settings.askTimeout)

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

  def bidiConnect(
    location1: Location, location2: Location,
    outboundFilter1: ReplicationFilter = NoFilter, outboundFilter2: ReplicationFilter = NoFilter,
    rfcLocations1: Set[Location]= Set.empty, negate1: Boolean = false,
    rfcLocations2: Set[Location]= Set.empty, negate2: Boolean = false
  ): Unit = {
    if(rfcLocations1.nonEmpty) location1.endpoint.addRedundantFilterConfig(location2.endpoint.id, RedundantFilterConfig(LogName, rfcLocations1.map(_.endpoint.id), !negate1))
    if(rfcLocations2.nonEmpty) location2.endpoint.addRedundantFilterConfig(location1.endpoint.id, RedundantFilterConfig(LogName, rfcLocations2.map(_.endpoint.id), !negate2))
    if(outboundFilter1 ne NoFilter) location1.endpoint.addTargetFilter(location2.endpoint.id, LogName, outboundFilter1)
    if(outboundFilter2 ne NoFilter) location2.endpoint.addTargetFilter(location1.endpoint.id, LogName, outboundFilter2)
    bidiConnect(location1, location2)
  }

  def expectPayloads(payloads: Seq[AnyRef], locs: Location*) =
    locs.foreach(_.expectPayloads(payloads))

  def locationMatrix(applicationNames: Seq[String], nReplicas: Int): Seq[Seq[Location]] = {
    val applications = applicationNames.map { applicationName =>
      (1 to nReplicas).map(replica => new Location(applicationName + replica))
    }
    // unfiltered connections between replicas of an application
    applications.foreach { application =>
      application.sliding(2).foreach(connected => bidiConnect(connected.head, connected.last))
    }
    // filtered connections between applications
    var rfcLocations = Set.empty[Location]
    for {
      Seq(app1, app2) <- applications.sliding(2)
      (location1, location2) <- app1 zip app2
    } {
      rfcLocations ++= app1
      bidiConnect(
        location1 = location1, outboundFilter1 = location1.filter, rfcLocations1 = rfcLocations, negate1 = true,
        location2 = location2, outboundFilter2 = location2.filter, rfcLocations2 = rfcLocations)
    }
    applications
  }
}

class Location(id: String) {

  import Location._

  var eventCnt = 0
  var emitted: List[AnyRef] = Nil
  val endpoint = new ReplicationEndpoint(s"EP-$id", Set(LogName))
  val probe = TestProbe(s"P-$id")(endpoint.system)
  val log = endpoint.eventLogs(LogName)
  log ! Subscribe(probe.ref)

  def emit(makePayloads: Function1[String, AnyRef]*): Seq[AnyRef] = {
    val payloads = makePayloads.toList.map { makePayload =>
      eventCnt += 1
      makePayload(s"$id.$eventCnt")
    }
    log ! Write(payloads.map(DecodedEvent(s"EM-$id", _)))
    emitted = payloads.reverse ::: emitted
    payloads
  }

  def emitN(makePayload: String => AnyRef, n: Int = 1): Seq[AnyRef] =
    emit(List.fill(n)(makePayload): _*)

  def emittedInternal = emitted.filter(_.isInstanceOf[InternalEvent])
  def emittedExternal = emitted.filter(_.isInstanceOf[ExternalEvent])

  def expectPayloads(payloads: Seq[AnyRef]): Unit =
    payloads.foreach { payload =>
      probe.expectMsgPF(hint = s"log ${ReplicationEndpoint.logId(endpoint.id, LogName)} expects $payload")(payloadEquals(payload))
    }

  def expectNoMsg(): Unit =
    probe.expectNoMsg(200.millis)

  def storedPayloads: Seq[AnyRef] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(log.ask(Read(0)).mapTo[ReadSuccess].map(_.events.map(_.payload)), timeout.duration)
  }

  val filter = replicationFilter(endpoint.system)

  override def toString = s"Loc:$id"
}

