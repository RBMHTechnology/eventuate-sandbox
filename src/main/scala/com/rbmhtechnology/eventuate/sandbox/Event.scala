package com.rbmhtechnology.eventuate.sandbox

case class EventMetadata(emitterId: String, emitterLogId: String, localLogId: String, localSequenceNr: Long, vectorTimestamp: VectorTime)
case class EventManifest(schema: String, majorVersion: Int, minorVersion: Int)
case class EventBytes(bytes: Array[Byte], serializerId: Int, manifest: EventManifest)

sealed trait DurableEvent {
  def metadata: EventMetadata

  def before(vectorTime: VectorTime): Boolean =
    metadata.vectorTimestamp <= vectorTime
}

object DecodedEvent {
  def apply(emitterId: String, payload: String): DecodedEvent =
    DecodedEvent(EventMetadata(emitterId, null, null, 0L, VectorTime.Zero), payload)
}

case class DecodedEvent(metadata: EventMetadata, payload: String) extends DurableEvent {
  // TODO: use serializers
  def encode: EncodedEvent =
    EncodedEvent(metadata, EventBytes(payload.getBytes("UTF-8"), 0, EventManifest("String", 0, 0)))
}

case class EncodedEvent(metadata: EventMetadata, payload: EventBytes) extends DurableEvent {
  // TODO: use serializers
  def decode: DecodedEvent =
    DecodedEvent(metadata, new String(payload.bytes, "UTF-8"))

  def emitted(localLogId: String, localSequenceNr: Long): EncodedEvent = {
    copy(metadata.copy(
      emitterLogId = localLogId,
      localLogId = localLogId,
      localSequenceNr = localSequenceNr,
      vectorTimestamp = metadata.vectorTimestamp.setLocalTime(localLogId, localSequenceNr)))
  }

  def replicated(localLogId: String, localSequenceNr: Long): EncodedEvent = {
    copy(metadata.copy(
      localLogId = localLogId,
      localSequenceNr = localSequenceNr))
  }
}
