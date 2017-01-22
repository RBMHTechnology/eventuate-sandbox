package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorSystem
import akka.util.Timeout
import com.rbmhtechnology.eventuate.sandbox.Location.ExternalEvent
import com.rbmhtechnology.eventuate.sandbox.Location.InternalEvent
import com.rbmhtechnology.eventuate.sandbox.Location.and
import com.rbmhtechnology.eventuate.sandbox.Location.bidiConnect
import com.rbmhtechnology.eventuate.sandbox.Location.disconnect
import com.rbmhtechnology.eventuate.sandbox.Location.expectPayloads
import com.rbmhtechnology.eventuate.sandbox.Location.fromLocation
import com.rbmhtechnology.eventuate.sandbox.Location.locationMatrix
import com.rbmhtechnology.eventuate.sandbox.Location.nTimes
import com.rbmhtechnology.eventuate.sandbox.Location.ofType
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.CompleteHistory
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.CurrentHistory
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.NoHistory
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.BeMatcher
import org.scalatest.matchers.MatchResult
import org.scalatest.time.Millis
import org.scalatest.time.Span

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

object LocationAdditionSpec {
  def atTheEndOf[A](as: Seq[A]): BeMatcher[Seq[A]] = BeMatcher { left =>
    MatchResult(
      left.equals(as.takeRight(left.size)),
      s"$left is not at the end of $as",
      s"$left is at the end of $as"
    )
  }
}

class LocationAdditionSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterEach {
  import LocationAdditionSpec._

  private val settings =
    new ReplicationSettings(ConfigFactory.load())

  implicit val timeout =
    Timeout(settings.askTimeout)

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(timeout.duration.toMillis, Millis), interval = Span(100, Millis))

  private var systems: Seq[ActorSystem] = Nil

  def registerLocation(location: Location): Location =
    registerLocations(List(location)).head

  def registerLocations(locations: Seq[Location]): Seq[Location] = {
    systems = locations.map(_.endpoint.system)
    locations
  }

  override protected def afterEach(): Unit =
    systems.foreach(_.terminate())

  "Added location" when {
    "starting replication with complete history" must {
      "replicate from locations without deleted events" in {
        val Seq(a1, a2) = registerLocations(locationMatrix(Seq("A"), 2).flatten)

        val fromA1 = a1.emitN(ExternalEvent, 2)
        expectPayloads(fromA1, a1, a2)
        a1.delete(1)

        val a3 = new Location("A3")

        bidiConnect(a2, a3, history = CompleteHistory)
        a3.expectPayloads(fromA1)

        bidiConnect(a1, a3, history = CurrentHistory)
        val fromA3 = a3.emit(ExternalEvent)
        expectPayloads(fromA3, a1, a3)

        disconnect(a2, a3)
        val newFromA1 = a1.emit(ExternalEvent)
        a3.expectPayloads(newFromA1)
      }
    }
    "starting replication with current history" must {
      "replicate from locations with deleted events" in {
        val Seq(a1, a2) = registerLocations(locationMatrix(Seq("A"), 2).flatten)

        val fromA1 = a1.emitN(ExternalEvent, 2)
        expectPayloads(fromA1, a1, a2)
        a1.delete(1)

        val a3 = new Location("A3")

        bidiConnect(a1, a3, history = CurrentHistory)
        a3.expectPayloads(fromA1.tail)
        val fromA3 = a3.emit(ExternalEvent)
        expectPayloads(fromA3, a1, a3)

        bidiConnect(a2, a3, history = CompleteHistory)
        a3.expectNoMsg()

        val newFromA2 = a2.emit(ExternalEvent)
        a3.expectPayloads(newFromA2)
      }
    }
    "replicating without history" must {
      "replicate only new events" in {
        // connect: A -> B (replicate from B to A)
        val a = registerLocation(new Location("A"))
        val b = registerLocation(new Location("B"))
        b.emit(ExternalEvent)

        a.connect(b, history = NoHistory).futureValue
        val newEvents = b.emit(ExternalEvent)
        a.expectPayloads(newEvents)
      }
    }
    "replicating without history from a location that already received local events" must {
      "replicate all local events" in {
        val Seq(a1, a2) = registerLocations(locationMatrix(Seq("A"), 2).flatten)
        val Seq(b1, b2) = registerLocations(locationMatrix(Seq("B"), 2).flatten)
        val bHistory = Seq(b1, b2).flatMap(_.emit(ExternalEvent))
        Seq(b1, b2).foreach(_.expectPayloads(bHistory.toSet))

        a1.connect(b1, history = NoHistory).futureValue
        disconnect(b1, b2)
        val fromB1 = b1.emit(ExternalEvent)
        a2.expectPayloads(fromB1)

        b2.connect(a2, history = NoHistory).futureValue
        bidiConnect(b1, b2)
        b2.expectPayloads(fromB1)
      }
    }
    "neighbor location already replicated without history" must {
      "replicate only new events" in {
        val Seq(a1, a2) = registerLocations(locationMatrix(Seq("A"), 2).flatten)
        val Seq(b1, b2) = registerLocations(locationMatrix(Seq("B"), 2).flatten)
        val bHistory = Seq(b1, b2).flatMap(_.emit(ExternalEvent))
        Seq(b1, b2).foreach(_.expectPayloads(bHistory.toSet))

        a1.connect(b1, history = NoHistory).futureValue
        val newB = Seq(b1, b2).flatMap(_.emit(ExternalEvent))
        a2.expectPayloads(newB)

        a2.connect(b2, history = CompleteHistory).futureValue
        val newB2 = b2.emit(ExternalEvent)
        a2.expectPayloads(newB2)
      }
    }
    "starting replication with complete history from a location that started with current history" must {
      "replicate first from a location with complete history" ignore {
        val Seq(a1, a2) = registerLocations(locationMatrix(Seq("A"), 2).flatten)

        val fromA1 = a1.emitN(ExternalEvent, 2)
        expectPayloads(fromA1, a1, a2)
        a1.delete(1)

        val a3 = new Location("A3")

        bidiConnect(a1, a3, history = CurrentHistory)
        a3.expectPayloads(fromA1.tail)

        val a4 = new Location("A4")
        bidiConnect(a4, a3, history = CompleteHistory)
        val fromA3 = a3.emit(ExternalEvent)
        // As DVV cannot be propagated properly A4 will receive events from A3
        a4.expectNoMsg()

        bidiConnect(a4, a2, history = CompleteHistory)
        a4.expectPayloads(fromA1 ++ fromA3)
      }
    }
    "replicating without history over RFC" must {
      "replicate only new remote events and all local events" in {
        val Seq(a1, a2) = registerLocations(locationMatrix(Seq("A"), 2).flatten)
        val Seq(b1, b2) = registerLocations(locationMatrix(Seq("B"), 2).flatten)
        Future {
          for {
            _ <- 1 to 150
            location <- List(a1, a2, b1, b2)
          } {
            location.emit(List.fill(1)(List(ExternalEvent, InternalEvent)).flatten: _*)
            Thread.sleep(3)
          }
        }
        a1.waitForPayload(nTimes(20)(fromLocation(a2)), "msgs from A2")
        a1.connect(b1, b1.filter, Set(a1, a2), history = NoHistory)

        b2.waitForNewPayload(nTimes(10)(fromLocation(b1)), "msgs from B1")
        b2.connect(a2, a2.filter, Set(b1, b2), history = NoHistory)

        b1.waitForNewPayload(nTimes(10)(fromLocation(a1)), "new msgs from A1")
        b1.connect(a1, a1.filter, Set(b1, b2), history = CompleteHistory)

        a2.waitForNewPayload(nTimes(10)(fromLocation(b2)), "new msgs from B2")
        a2.connect(b2, b2.filter, Set(a1, a2), history = CompleteHistory)

        a1.waitForIdle(200.millis)
        a2.waitForIdle(200.millis)
        b1.waitForIdle(200.millis)
        b2.waitForIdle(200.millis)
        a1.storedPayloads should contain theSameElementsAs a2.storedPayloads
        b1.storedPayloads should contain theSameElementsAs b2.storedPayloads

        def externalFrom(l: Location): AnyRef => Boolean = and(fromLocation(l), ofType[ExternalEvent])

        b1.storedPayloads.filter(fromLocation(a1)) shouldBe atTheEndOf(a1.storedPayloads.filter(externalFrom(a1)))
        b1.storedPayloads.filter(fromLocation(a2)) shouldBe atTheEndOf(a2.storedPayloads.filter(externalFrom(a2)))
        a1.storedPayloads.filter(fromLocation(b1)) shouldBe atTheEndOf(b1.storedPayloads.filter(externalFrom(b1)))
        a1.storedPayloads.filter(fromLocation(b2)) shouldBe atTheEndOf(b2.storedPayloads.filter(externalFrom(b2)))
      }
    }
  }

  "Added location" must {
    "replicate all events, if events are deleted after connect" in {
      val a1 = registerLocation(new Location("A1"))
      val emitted = a1.emitN(ExternalEvent, 2)
      val a2 = registerLocation(new Location("A2"))
      a2.endpoint.connect(a1.endpoint).futureValue
      a1.delete(1)
      a2.expectPayloads(emitted)
    }
  }
}
