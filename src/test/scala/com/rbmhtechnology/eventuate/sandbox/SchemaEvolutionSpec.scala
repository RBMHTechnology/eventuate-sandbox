package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.Incompatible
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.MajorIncompatibility
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.MinorIncompatibility
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.eventCompatibilityDecider
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Subscribe
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Write
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Block
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Continue
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Filter
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpec

import scala.concurrent.duration.DurationInt

object SchemaEvolutionSpec {
  val EmitterId1 = "EM1"
  val EmitterId2 = "EM2"
  val EndpointId1 = "EP1"
  val EndpointId2 = "EP2"

  val LogName = "L"

  def serializerConfig(serializerClass: Class[_]) =
    ConfigFactory.parseString(
      s"""
         |akka.actor {
         |  serializers {
         |    test-event = "${serializerClass.getName}"
         |  }
         |  serialization-bindings {
         |    "${classOf[Format].getName}" = test-event
         |  }
         |}
      """.stripMargin)

  trait Format extends Serializable
  case object CompatibleEvent extends Format
  case object MinorIncompatibleEvent extends Format
  case object MajorIncompatibleEvent extends Format
  case object NoVersionEvent


  abstract class TestSerializer extends EventPayloadSerializer {
    protected val CompatibleEventManifest = CompatibleEvent.toString
    protected val MinorIncompatibleEventManifest = MinorIncompatibleEvent.toString
    protected val MajorIncompatibleEventManifest = MajorIncompatibleEvent.toString

    override def identifier: Int = 896798

    override def manifest(o: AnyRef): String =
      o.toString

    override def toBinary(o: AnyRef): Array[Byte] =
      Array.empty[Byte]

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      manifest match {
        case CompatibleEventManifest => CompatibleEvent
        case MinorIncompatibleEventManifest => MinorIncompatibleEvent
        case MajorIncompatibleEventManifest => MajorIncompatibleEvent
      }
  }

  case class TestSerializer1(system: ExtendedActorSystem) extends TestSerializer {
    override def payloadVersion(schema: String): PayloadVersion =
      schema match {
        case CompatibleEventManifest => PayloadVersion(1,1)
        case MinorIncompatibleEventManifest => PayloadVersion(1,2)
        case MajorIncompatibleEventManifest => PayloadVersion(2,1)
      }
  }

  case class TestSerializer2(system: ExtendedActorSystem) extends TestSerializer {
    override def payloadVersion(schema: String) =
      PayloadVersion(1,1)
  }

  def stopOnUnexpectedFilterMajorContinueOnMinor(implicit system: ActorSystem): ReplicationDecider =
    eventCompatibilityDecider {
      case _: MajorIncompatibility => Filter
      case _: MinorIncompatibility => Continue
      case incompatibility => Block(Incompatible(incompatibility))
    }

  def payloadEquals(payload: AnyRef): PartialFunction[Any, Any] = {
    case DecodedEvent(_, actual) if actual == payload => actual
  }
}

class SchemaEvolutionSpec extends WordSpec with Matchers with BeforeAndAfterEach {

  import SchemaEvolutionSpec._

  private var endpoint1: ReplicationEndpoint = _
  private var endpoint2: ReplicationEndpoint = _
  private var probe1: TestProbe = _
  private var probe2: TestProbe = _
  private var log1: ActorRef = _
  private var log2: ActorRef = _

  override protected def beforeEach(): Unit = {
    endpoint1 = new ReplicationEndpoint(EndpointId1, Set(LogName), config = serializerConfig(classOf[TestSerializer1]))
    endpoint2 = new ReplicationEndpoint(EndpointId2, Set(LogName), config = serializerConfig(classOf[TestSerializer2]))

    probe1 = TestProbe()(endpoint1.system)
    probe2 = TestProbe()(endpoint2.system)

    log1 = endpoint1.eventLogs(LogName)
    log2 = endpoint2.eventLogs(LogName)
    log1 ! Subscribe(probe1.ref)
    log2 ! Subscribe(probe2.ref)

    endpoint1.connect(endpoint2, Map(LogName -> stopOnUnexpectedFilterMajorContinueOnMinor(endpoint1.system)))
    endpoint2.connect(endpoint1, Map(LogName -> stopOnUnexpectedFilterMajorContinueOnMinor(endpoint2.system)))
  }

  override protected def afterEach(): Unit = {
    endpoint1.terminate()
    endpoint2.terminate()
  }

  "ReplicationEndpoint" must {
    "replicate event from new to old location based on event compatibility" in {
      log1 ! Write(List(DecodedEvent(EmitterId1, MajorIncompatibleEvent)))
      // filtered

      log1 ! Write(List(DecodedEvent(EmitterId1, CompatibleEvent)))
      probe2.expectMsgPF(hint = CompatibleEvent.toString)(payloadEquals(CompatibleEvent))

      log1 ! Write(List(DecodedEvent(EmitterId1, MinorIncompatibleEvent)))
      probe2.expectMsgPF(hint = MinorIncompatibleEvent.toString)(payloadEquals(MinorIncompatibleEvent))

      log1 ! Write(List(DecodedEvent(EmitterId1, NoVersionEvent)))
      // Blocked

      log1 ! Write(List(DecodedEvent(EmitterId1, CompatibleEvent)))
      probe2.expectNoMsg(500.millis) // Still blocked
    }
    "replicate event from old to new location based on event compatibility" in {
      log2 ! Write(List(DecodedEvent(EmitterId2, MajorIncompatibleEvent)))
      probe1.expectMsgPF(hint = MajorIncompatibleEvent.toString)(payloadEquals(MajorIncompatibleEvent))

      log2 ! Write(List(DecodedEvent(EmitterId2, CompatibleEvent)))
      probe1.expectMsgPF(hint = CompatibleEvent.toString)(payloadEquals(CompatibleEvent))

      log2 ! Write(List(DecodedEvent(EmitterId2, MinorIncompatibleEvent)))
      probe1.expectMsgPF(hint = MinorIncompatibleEvent.toString)(payloadEquals(MinorIncompatibleEvent))

      log2 ! Write(List(DecodedEvent(EmitterId2, NoVersionEvent)))
      // Blocked

      log2 ! Write(List(DecodedEvent(EmitterId2, CompatibleEvent)))
      probe1.expectNoMsg(500.millis) // Still blocked
    }
  }
}
