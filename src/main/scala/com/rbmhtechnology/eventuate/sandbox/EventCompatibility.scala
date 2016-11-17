package com.rbmhtechnology.eventuate.sandbox

import akka.serialization.Serialization
import akka.serialization.Serializer
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer

import scala.reflect.ClassTag
import scala.reflect._

trait EventCompatibility
// TODO add DecodedEvent to each EncodedEvent
// TODO add ExceptionOnDecode
case class Compatible(event: EncodedEvent) extends EventCompatibility
case class MinorIncompatibility(event: EncodedEvent, required: EventVersion, supported: EventVersion) extends EventCompatibility
case class MajorIncompatibility(schema: String, required: EventVersion, supported: EventVersion) extends EventCompatibility
case class NoSerializer(serializerId: Int) extends EventCompatibility
case class NotEventPayloadSerializer(event: EncodedEvent, serializer: Serializer) extends EventCompatibility
case class NoEventVersion(event: EncodedEvent, serializerId: Int) extends EventCompatibility

object EventCompatibility {

  def eventCompatibility(event: EncodedEvent)(implicit serialization: Serialization): EventCompatibility = {
    val serializerId = event.payload.serializerId
    val manifest = event.payload.manifest
    val compatibility= for {
      serializer <- toRight(serialization.serializerByIdentity.get(serializerId), NoSerializer(serializerId))
      payloadSerializer <- castOrLeft[EventPayloadSerializer, EventCompatibility](serializer, NotEventPayloadSerializer(event, serializer))
      eventVersion <- toRight(manifest.eventVersion, NoEventVersion(event, serializerId))
      _ <- Left(compareVersions(event, payloadSerializer.eventVersion(manifest.schema), eventVersion)).right
    } yield ()
    compatibility.left.get
  }

  private def compareVersions(event: EncodedEvent, supported: EventVersion, required: EventVersion): EventCompatibility = {
    if(supported.majorVersion < required.majorVersion) MajorIncompatibility(event.payload.manifest.schema, required, supported)
    else if(supported.majorVersion == required.majorVersion && supported.minorVersion < required.minorVersion) MinorIncompatibility(event, required, supported)
    else Compatible(event)
  }

  private def castOrLeft[A : ClassTag, L](a: AnyRef, left: L): Either.RightProjection[L, A] =
    Either.cond(classTag[A].runtimeClass.isAssignableFrom(a.getClass), a.asInstanceOf[A], left).right

  private def toRight[L, R](option: Option[R], left: L): Either.RightProjection[L, R] =
    Either.cond(option.isDefined, option.get, left).right
}
