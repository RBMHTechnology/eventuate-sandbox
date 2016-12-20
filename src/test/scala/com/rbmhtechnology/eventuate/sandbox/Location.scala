package com.rbmhtechnology.eventuate.sandbox

import java.lang.System.nanoTime

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Read
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.ReadSuccess
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Subscribe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Write
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.CompleteHistory
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.ReplicationHistory
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.reflect.classTag

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

  def fromLocation(location: Location): AnyRef => Boolean = {
    case ExternalEvent(s) => s.startsWith(location.id)
    case InternalEvent(s) => s.startsWith(location.id)
    case _ => false
  }

  def ofType[A: ClassTag]: AnyRef => Boolean = { a =>
    classTag.runtimeClass.isAssignableFrom(a.getClass)
  }

  def and[A](cond1: A => Boolean, cond2: A => Boolean): A => Boolean = a => cond1(a) && cond2(a)

  def nTimes[A](n: Int)(condition: A => Boolean = (_: A) => true): A => Boolean = new (A => Boolean) {
    var counter: Int = 0
    override def apply(a: A) = {
      if (condition(a)) counter += 1
      counter >= n
    }
  }

  def disconnect(location1: Location, location2: Location): Unit = {
    location1.endpoint.disconnect(location2.endpoint.id)
    location2.endpoint.disconnect(location1.endpoint.id)
  }

  def bidiConnect(
    location1: Location, location2: Location,
    outboundFilter1: ReplicationFilter = NoFilter, outboundFilter2: ReplicationFilter = NoFilter,
    rfcLocations1: Set[Location]= Set.empty, negate1: Boolean = false,
    rfcLocations2: Set[Location]= Set.empty, negate2: Boolean = false,
    history: ReplicationHistory = CompleteHistory
  ): Unit = {
    location1.connect(location2, outboundFilter2, rfcLocations2, negate2, history)
    location2.connect(location1, outboundFilter1, rfcLocations1, negate1, history)
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

class Location(val id: String) extends Matchers {
  import Location._

  var eventCnt = 0
  var emitted: List[AnyRef] = Nil
  val endpoint = new ReplicationEndpoint(s"EP-$id", Set(LogName))
  val probe = TestProbe(s"P-$id")(endpoint.system)
  val log = endpoint.eventLogs(LogName)
  log ! Subscribe(probe.ref)

  def connect(
    location: Location,
    inboundFilter: ReplicationFilter = NoFilter,
    rfcLocations: Set[Location] = Set.empty, rfcNegate: Boolean = false,
    history: ReplicationHistory = CompleteHistory
  ): Future[String] = {
    if(rfcLocations.nonEmpty) location.endpoint.addRedundantFilterConfig(endpoint.id, RedundantFilterConfig(LogName, rfcLocations.map(_.endpoint.id), !rfcNegate))
    if(inboundFilter ne NoFilter) location.endpoint.addTargetFilter(endpoint.id, LogName, inboundFilter)
    endpoint.connect(location.endpoint, history)
  }

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
      probe.expectMsgPF(hint = s"log ${ReplicationEndpoint.logId(endpoint.id, LogName)} expects $payload", max = timeout.duration)(payloadEquals(payload))
    }

  def expectPayloads(payloads: Set[AnyRef]): Unit = {
    val received = probe.receiveN(payloads.size, timeout.duration)
    received.map { case DecodedEvent(_, actual) => actual } should contain theSameElementsAs payloads
  }

  def expectNoMsg(): Unit =
    probe.expectNoMsg(200.millis)

  def waitForPayload(payload: AnyRef): Unit =
    waitForPayload(_ == payload, hint = payload.toString)

  def waitForPayload(condition: AnyRef => Boolean, hint: String = "payload meeting generic condition"): Unit =
    waitForPayload(probe, condition, hint)

  def waitForNewPayload(condition: AnyRef => Boolean, hint: String) = {
    val probe = TestProbe()(endpoint.system)
    log ! Subscribe(probe.ref)
    waitForPayload(probe, condition, hint)
  }

  private def waitForPayload(probe: TestProbe, condition: AnyRef => Boolean, hint: String): Unit = {
    probe.fishForMessage(hint = s"log ${ReplicationEndpoint.logId(endpoint.id, LogName)} waits for $hint", max = timeout.duration) {
      case DecodedEvent(_, actual) => condition(actual)
    }
  }

  def waitForIdle(idle: FiniteDuration): Unit =
    probe.receiveWhile(max = timeout.duration, idle = idle) { case m => m }

  def storedPayloads: Seq[AnyRef] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(log.ask(Read(0)).mapTo[ReadSuccess].map(_.events.map(_.payload)), timeout.duration)
  }

  def delete(toSequenceNo: Long): Unit =
    endpoint.delete(LogName, toSequenceNo)

  val filter = replicationFilter(endpoint.system)

  override def toString = s"Loc:$id"
}

