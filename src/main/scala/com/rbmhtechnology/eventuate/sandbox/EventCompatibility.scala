package com.rbmhtechnology.eventuate.sandbox

import akka.serialization.Serialization
import akka.serialization.Serializer
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer.decode

import scala.reflect.ClassTag
import scala.reflect._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait EventCompatibility
case class Compatible(event: FullEvent) extends EventCompatibility
case class MinorIncompatibility(event: FullEvent, required: EventVersion, supported: EventVersion) extends EventCompatibility
case class MajorIncompatibility(schema: String, required: EventVersion, supported: EventVersion) extends EventCompatibility
case class CannotDecode(serializer: Serializer, schema: String, cause: Throwable) extends EventCompatibility
case class NoSerializer(serializerId: Int) extends EventCompatibility
case class NotEventPayloadSerializer(event: FullEvent, serializer: Serializer) extends EventCompatibility
case class NoEventVersion(event: FullEvent, serializerId: Int) extends EventCompatibility

object EventCompatibility {

  def eventCompatibility(encoded: EncodedEvent)(implicit serialization: Serialization): EventCompatibility = {
    val serializerId = encoded.payload.serializerId
    val manifest = encoded.payload.manifest
    val compatibility= for {
      serializer <- toRight(serialization.serializerByIdentity.get(serializerId), NoSerializer(serializerId))
      event <- toRight(decode(encoded).map(FullEvent(encoded, _)), CannotDecode(serializer, manifest.schema, _ : Throwable))
      payloadSerializer <- castOrLeft[EventPayloadSerializer, EventCompatibility](serializer, NotEventPayloadSerializer(event, serializer))
      eventVersion <- toRight(manifest.eventVersion, NoEventVersion(event, serializerId))
      _ <- Left(compareVersions(event, payloadSerializer.eventVersion(manifest.schema), eventVersion)).right
    } yield ()
    compatibility.left.get
  }

  private def compareVersions(event: FullEvent, supported: EventVersion, required: EventVersion): EventCompatibility = {
    if(supported.majorVersion < required.majorVersion) MajorIncompatibility(event.encoded.payload.manifest.schema, required, supported)
    else if(supported.majorVersion == required.majorVersion && supported.minorVersion < required.minorVersion) MinorIncompatibility(event, required, supported)
    else Compatible(event)
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
}
