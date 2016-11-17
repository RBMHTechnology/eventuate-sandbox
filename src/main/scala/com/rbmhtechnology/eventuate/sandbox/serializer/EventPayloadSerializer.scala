package com.rbmhtechnology.eventuate.sandbox.serializer

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.serialization.Serializer
import akka.serialization.SerializerWithStringManifest
import com.rbmhtechnology.eventuate.sandbox.DecodedEvent
import com.rbmhtechnology.eventuate.sandbox.EncodedEvent
import com.rbmhtechnology.eventuate.sandbox.EventBytes
import com.rbmhtechnology.eventuate.sandbox.EventManifest
import com.rbmhtechnology.eventuate.sandbox.EventVersion

import scala.util.Try

abstract class EventPayloadSerializer extends SerializerWithStringManifest {
  def eventVersion(schema: String): EventVersion
}

object EventPayloadSerializer {
  def encode(event: DecodedEvent)(implicit system: ActorSystem): EncodedEvent =
    EncodedEvent(event.metadata, serializePayload(event.payload))

  def decode(event: EncodedEvent)(implicit system: ActorSystem): Try[DecodedEvent] =
    deserializePayload(event.payload)
      .map(payload => DecodedEvent(event.metadata, payload))

  def isDeserializable(event: EncodedEvent)(implicit system: ActorSystem): Boolean =
    deserializePayload(event.payload).isSuccess

  private def serializePayload(payload: AnyRef)(implicit system: ActorSystem): EventBytes = {
    val serializer = SerializationExtension(system).findSerializerFor(payload)
    EventBytes(serializer.toBinary(payload), serializer.identifier, eventManifest(serializer, payload))
  }

  private def eventManifest(serializer: Serializer, payload: AnyRef): EventManifest = {
    val schema = eventSchema(serializer, payload)
    EventManifest(schema, serializer.isInstanceOf[SerializerWithStringManifest], eventVersion(serializer, schema))
  }

  private def eventSchema(serializer: Serializer, payload: AnyRef): String =
    serializer match {
      case serializerWithStringManifest: SerializerWithStringManifest =>
        serializerWithStringManifest.manifest(payload)
      case _ =>
        payload.getClass.getName
    }

  private def eventVersion(serializer: Serializer, schema: String): Option[EventVersion] =
    serializer match {
      case payloadSerializer: EventPayloadSerializer => Some(payloadSerializer.eventVersion(schema))
      case _ => None
    }

  private def deserializePayload(payload: EventBytes)(implicit system: ActorSystem): Try[AnyRef] =
    if (payload.manifest.isStringManifest) {
      SerializationExtension(system).deserialize(
        payload.bytes,
        payload.serializerId,
        payload.manifest.schema)
    } else {
      val manifestClass = Class.forName(payload.manifest.schema)
      SerializationExtension(system).deserialize(payload.bytes, manifestClass).map(_.asInstanceOf[AnyRef])
    }
}