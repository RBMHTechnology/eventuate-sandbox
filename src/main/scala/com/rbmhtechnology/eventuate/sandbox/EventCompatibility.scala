package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Block
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Continue
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.ReplicationDecision
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer.decode

import scala.reflect.ClassTag
import scala.reflect._
import scala.util.Failure
import scala.util.Success
import scala.util.Try


object EventCompatibility {
  sealed trait IncompatibilityReason
  case class MinorIncompatibility(event: EncodedEvent, required: PayloadVersion, supported: PayloadVersion) extends IncompatibilityReason
  case class MajorIncompatibility(schema: String, required: PayloadVersion, supported: PayloadVersion) extends IncompatibilityReason
  case class FailureOnDeserialization(serializerId: Int, schema: String, cause: Throwable) extends IncompatibilityReason
  case class NoSerializer(serializerId: Int) extends IncompatibilityReason
  case class NoLocalPayloadVersion(event: EncodedEvent, serializerId: Int) extends IncompatibilityReason
  case class NoRemotePayloadVersion(event: EncodedEvent, serializerId: Int) extends IncompatibilityReason

  def eventCompatibility(encoded: EncodedEvent)(implicit system: ActorSystem): Option[IncompatibilityReason]= {
    val serializerId = encoded.payload.serializerId
    val manifest = encoded.payload.manifest
    val compatibility= for {
      serializer <- toRight(SerializationExtension(system).serializerByIdentity.get(serializerId), NoSerializer(serializerId))
      event <- toRight(decode(encoded).map(_ => encoded), FailureOnDeserialization(serializerId, manifest.schema, _ : Throwable))
      payloadSerializer <- castOrLeft[EventPayloadSerializer, IncompatibilityReason](serializer, NoLocalPayloadVersion(event, serializerId))
      payloadVersion <- toRight(manifest.payloadVersion, NoRemotePayloadVersion(event, serializerId))
      _ <- compareVersions(event, payloadSerializer.payloadVersion(manifest.schema), payloadVersion)
    } yield ()
    compatibility.left.toOption
  }

  private def compareVersions(event: EncodedEvent, supported: PayloadVersion, required: PayloadVersion): Either.RightProjection[IncompatibilityReason, Unit] = {
    val res = if(supported.majorVersion < required.majorVersion)
      Left(MajorIncompatibility(event.payload.manifest.schema, required, supported))
    else if(supported.majorVersion == required.majorVersion && supported.minorVersion < required.minorVersion)
      Left(MinorIncompatibility(event, required, supported))
    else
      Right(())
    res.right
  }

  private def castOrLeft[A : ClassTag, L](a: AnyRef, left: L): Either.RightProjection[L, A] =
    Either.cond(classTag[A].runtimeClass.isAssignableFrom(a.getClass), a.asInstanceOf[A], left).right

  private def toRight[L, R](option: Option[R], left: L): Either.RightProjection[L, R] =
    Either.cond(option.isDefined, option.get, left).right

  private def toRight[L, R](t: Try[R], makeLeft: Throwable => L): Either.RightProjection[L, R] =
    t match {
      case Success(r) => Right(r).right
      case Failure(ex) => Left(makeLeft(ex)).right
    }

  def eventCompatibilityDecider(decider: IncompatibilityReason => ReplicationDecision)(implicit system: ActorSystem): ReplicationDecider =
    new ReplicationDecider {
      override def apply(event: EncodedEvent) =
        eventCompatibility(event).map(decider).getOrElse(Continue)
    }

  case class Incompatible(compatibility: IncompatibilityReason) extends BlockReason

  def stopOnIncompatibility(implicit system: ActorSystem) = eventCompatibilityDecider {
    incompatibility => Block(Incompatible(incompatibility))
  }

  def stopOnUnserializableKeepOthers(implicit system: ActorSystem) = eventCompatibilityDecider {
    case _: MinorIncompatibility | _: NoRemotePayloadVersion | _: NoLocalPayloadVersion => Continue
    case incompatibility => Block(Incompatible(incompatibility))
  }
}
