package com.rbmhtechnology.eventuate.sandbox

import akka.util.Timeout
import com.rbmhtechnology.eventuate.sandbox.Location.ExternalEvent
import com.rbmhtechnology.eventuate.sandbox.Location.InternalEvent
import com.rbmhtechnology.eventuate.sandbox.Location.LogName
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Millis
import org.scalatest.time.Span

class EventDeletionSpec extends WordSpec with Matchers with BeforeAndAfterEach with Eventually {

  private val settings =
    new ReplicationSettings(ConfigFactory.load())

  implicit val timeout =
    Timeout(settings.askTimeout)

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(timeout.duration.toMillis, Millis), interval = Span(100, Millis))

  "ReplicationEndpoints" must {
    "delete events when replicated" in {
      val Seq(a1, a2) = Location.locationMatrix(List("A"), 2).flatten

      val initialEmission = a1.emit(ExternalEvent)
      a2.expectPayloads(initialEmission)

      val emitted = a1.emitN(ExternalEvent, 2)
      a1.endpoint.delete(LogName, 2)
      a2.expectPayloads(emitted)
      eventually(a1.storedPayloads shouldBe emitted.tail)
    }
    "delete events when replication is filtered" in {
      // fails as the internal event currently cannot be deleted
      // as following events are not causally dependent
      // and the internal event is not replicated to b2
      // thus version vector of b2 is never >= timestamp(internal event)
      val Seq(a1, a2, b1, b2) = Location.locationMatrix(List("A", "B"), 2).flatten

      val internal = a1.emit(InternalEvent)
      a2.expectPayloads(internal)

      val external = a2.emit(ExternalEvent)
      b2.expectPayloads(external)

      a2.endpoint.delete(LogName, 1)
      eventually(a2.storedPayloads shouldBe external)
    }
  }
}
