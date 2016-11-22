package com.rbmhtechnology.eventuate.sandbox.serializer

import akka.serialization.Serialization
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
  def encode(event: DecodedEvent)(implicit serialization: Serialization): EncodedEvent =
    EncodedEvent(event.metadata, serializePayload(event.payload))

  def decode(event: EncodedEvent)(implicit serialization: Serialization): Try[DecodedEvent] =
    deserializePayload(event.payload)
      .map(payload => DecodedEvent(event.metadata, payload))

  def isDeserializable(event: EncodedEvent)(implicit serialization: Serialization): Boolean =
    deserializePayload(event.payload).isSuccess

  private def serializePayload(payload: AnyRef)(implicit serialization: Serialization): EventBytes = {
    val serializer = serialization.findSerializerFor(payload)
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

  private def deserializePayload(payload: EventBytes)(implicit serialization: Serialization): Try[AnyRef] =
    if (payload.manifest.isStringManifest) {
      serialization.deserialize(
        payload.bytes,
        payload.serializerId,
        payload.manifest.schema)
    } else {
      val manifestClass = Class.forName(payload.manifest.schema)
      serialization.deserialize(payload.bytes, manifestClass).map(_.asInstanceOf[AnyRef])
    }
}