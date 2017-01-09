package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.sandbox.Location._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Millis
import org.scalatest.time.Span

import scala.collection.immutable.Seq
import scala.util.Random

object RedundantFilteredConnectionsSpec {

  def randomDisconnects(disconnected: Vector[(Location, Location)], applications: Seq[Seq[Location]]): Vector[(Location, Location)] = {
    val Seq(loc1, loc2) = Random.shuffle(Random.shuffle(applications).head.sliding(2).toList).head
    disconnect(loc1, loc2)
    val updated = uniqueAppend(disconnected, (loc1, loc2))
    reconnectFirstIfMoreThan(updated, (applications.head.size - 1) * applications.size / 3)
  }

  private def uniqueAppend(disconnected: Vector[(Location, Location)], locations: (Location, Location)) =
    disconnected.filterNot(_ == locations) :+ locations

  private def reconnectFirstIfMoreThan(updated: Vector[(Location, Location)], n: Int) =
    if (updated.size >= n) {
      bidiConnect _ tupled updated.head
      updated.tail
    } else
      updated
}

class RedundantFilteredConnectionsSpec extends WordSpec with Matchers with BeforeAndAfterEach with Eventually {

  import RedundantFilteredConnectionsSpec._

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(Location.timeout.duration.toMillis, Millis), interval = Span(100, Millis))

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

  private def awaitEventDistributionWithRandomDisconnects(applications: Seq[Seq[Location]]) = {
    val locations = applications.flatten
    val firstLocation = locations.head
    val lastEmitted = locations.last.emittedExternal.head
    var disconnected = Vector.empty[(Location, Location)]
    var i = 0
    firstLocation.probe.fishForMessage(hint = s"${locations.head.id} fish $lastEmitted", max = Location.timeout.duration) {
      case ev: DecodedEvent if ev.payload == lastEmitted => true
      case _ =>
        i += 1
        if (i % 10 == 0) disconnected = randomDisconnects(disconnected, applications)
        continueIfDisconnected(firstLocation, disconnected)
        false
    }
    disconnected.foreach(bidiConnect _ tupled _)
  }

  private def continueIfDisconnected(location:Location, disconnected: Seq[(Location, Location)]) =
    if (disconnected.exists { case (loc1, loc2) => (loc1 eq location) || (loc2 eq location) })
      location.probe.ref ! "Continue"
}
