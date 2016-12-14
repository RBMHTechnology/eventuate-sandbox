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
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Millis
import org.scalatest.time.Span

import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.util.Random

object RedundantFilteredConnectionsSpec {
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

  def event(payload: AnyRef): DecodedEvent = DecodedEvent("emitter-id", payload)

  class Location(id: String) {
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
        probe.expectMsgPF(hint = s"${probe.ref} expects $payload")(payloadEquals(payload))
      }

    def expectNoMsg(): Unit =
      probe.expectNoMsg(200.millis)

    def storedPayloads: Seq[AnyRef] =
      Await.result(log.ask(Read(0)).mapTo[ReadSuccess].map(_.events.map(_.payload)), timeout.duration)

    val filter = replicationFilter(endpoint.system)

    override def toString = s"Loc:$id"
  }

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

  def randomDisconnects(disconnected: Vector[(Location, Location)], applications: Seq[Seq[Location]]): Vector[(Location, Location)] = {
    val Seq(loc1, loc2) = Random.shuffle(Random.shuffle(applications).head.sliding(2).toList).head
    disconnect(loc1, loc2)
    val updated = disconnected.filterNot(_ == (loc1, loc2)) :+ (loc1, loc2)
    if(updated.size >= (applications.head.size - 1) * applications.size / 3) {
      bidiConnect _ tupled updated.head
      updated.tail
    } else
      updated
  }
}

class RedundantFilteredConnectionsSpec extends WordSpec with Matchers with BeforeAndAfterEach with Eventually {

  import RedundantFilteredConnectionsSpec._

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(RedundantFilteredConnectionsSpec.timeout.duration.toMillis, Millis), interval = Span(100, Millis))

  private var systems: Seq[ActorSystem] = Nil

  def newLocations(ids: String*): Seq[Location] =
    registerLocations(ids.toList.map(i => new Location(i)))

  def registerLocations(locations: Seq[Location]): Seq[Location] = {
    systems = locations.map(_.endpoint.system)
    locations
  }

  override protected def afterEach(): Unit =
    systems.foreach(_.terminate())

  "ReplicationEndpoint" must {
    "stop replication over redundant filtered from A to replicated application B1, B2" in {
      //    A
      //   / \ (RFC)
      // B1 - B2
      // B1, B2 initially interrupted
      val Seq(a, b1, b2)= newLocations("A", "B1", "B2")
      val redundantConnectionsA = Set(b1, b2)
      bidiConnect(a, b1, outboundFilter2 = b1.filter, rfcLocations1 = redundantConnectionsA)
      bidiConnect(a, b2, outboundFilter2 = b2.filter, rfcLocations1 = redundantConnectionsA)

      expectPayloads(a.emit(ExternalEvent), a, b1, b2)

      val fromB1 = b1.emit(InternalEvent, ExternalEvent)
      expectPayloads(fromB1.filter(_.isInstanceOf[ExternalEvent]), a)

      val fromA = a.emit(ExternalEvent)
      b2.expectNoMsg()

      bidiConnect(b2, b1)

      expectPayloads(fromB1 ++ fromA, b2)
    }
    "stop replication over redundant filtered from replicated application A1, A2 to replicated application B1, B2 and vice versa" in {
      val Seq(a1, a2, b1, b2) = registerLocations(locationMatrix(List("A", "B"), 2).flatten)
      disconnect(a1, a2)

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
    "replicate events properly over multiple RFC" in {
      //   RFC  RFC
      // A1 - B1 - C1 ...
      // |    |    |
      // A2 - B2 - ...
      // |    |
      // A3 - ...
      // ...
      val applications = locationMatrix(List("A", "B", "C", "D"), 4)
      val locations = registerLocations(applications.flatten)

      for {
        _ <- 1 to 20
        location <- locations
      } location.emit(List.fill(6)(List(ExternalEvent, InternalEvent)).flatten: _*)

      awaitEventDistributionWithRandomDisconnects(applications)

      val allExternal = locations.flatMap(_.emittedExternal).toSet
      for {
        application <- applications
        allApplicationInternal = application.flatMap(_.emittedInternal).toSet
        replica <- application
      } eventually {
        replica.storedPayloads.toSet shouldBe allExternal ++ allApplicationInternal
      }
    }
  }

  def awaitEventDistributionWithRandomDisconnects(applications: Seq[Seq[Location]]) = {
    val locations = applications.flatten
    val lastEmitted = locations.last.emittedExternal.head
    var disconnected = Vector.empty[(Location, Location)]
    var i = 0
    locations.head.probe.fishForMessage(hint = s"${locations.head.endpoint.id} fish $lastEmitted", max = RedundantFilteredConnectionsSpec.timeout.duration) {
      case ev: DecodedEvent if ev.payload == lastEmitted => true
      case _ =>
        i += 1
        if (i % 10 == 0) disconnected = randomDisconnects(disconnected, applications)
        false
    }
    disconnected.foreach(bidiConnect _ tupled _)
  }
}
