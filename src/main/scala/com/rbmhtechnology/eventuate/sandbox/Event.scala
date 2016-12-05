package com.rbmhtechnology.eventuate.sandbox

case class EventManifest(schema: String, majorVersion: Int, minorVersion: Int)
case class EventBytes(bytes: Array[Byte], serializerId: Int, manifest: EventManifest)

case class EventMetadata(emitterId: String, emitterLogId: String, localLogId: String, localSequenceNr: Long, vectorTimestamp: VectorTime) {
  def emitted(logId: String, currentLocalTime: Long): EventMetadata =
    copy(emitterLogId = logId, localLogId = logId, localSequenceNr = currentLocalTime, vectorTimestamp = vectorTimestamp.setLocalTime(logId, currentLocalTime))

  def replicated(logId: String, currentLocalTime: Long): EventMetadata =
    copy(localLogId = logId, localSequenceNr = currentLocalTime)
}

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
  def encode: EncodedEvent =
    EncodedEvent(metadata, EventBytes(payload.getBytes("UTF-8"), 0, EventManifest("String", 0, 0)))
}

case class EncodedEvent(metadata: EventMetadata, payload: EventBytes) extends DurableEvent {
  def decode: DecodedEvent =
    DecodedEvent(metadata, new String(payload.bytes, "UTF-8"))

  def emitted(logId: String, currentLocalTime: Long): EncodedEvent =
    copy(metadata.emitted(logId, currentLocalTime))

  def replicated(logId: String, currentLocalTime: Long): EncodedEvent =
    copy(metadata.replicated(logId, currentLocalTime))
}
