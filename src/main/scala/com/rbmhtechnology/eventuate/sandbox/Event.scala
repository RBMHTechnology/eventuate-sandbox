package com.rbmhtechnology.eventuate.sandbox

case class EventMetadata(emitterId: String, emitterLogId: String, localLogId: String, localSequenceNo: Long, vectorTimestamp: VectorTime)
case class PayloadVersion(majorVersion: Int, minorVersion: Int)
case class EventManifest(schema: String, isStringManifest: Boolean, payloadVersion: Option[PayloadVersion])
case class EventBytes(bytes: Array[Byte], serializerId: Int, manifest: EventManifest)

sealed trait DurableEvent {
  def metadata: EventMetadata

  def before(vectorTime: VectorTime): Boolean =
    metadata.vectorTimestamp <= vectorTime
}

object DecodedEvent {
  def apply(emitterId: String, payload: AnyRef): DecodedEvent =
    DecodedEvent(EventMetadata(emitterId, null, null, 0L, VectorTime.Zero), payload)
}

case class DecodedEvent(metadata: EventMetadata, payload: AnyRef) extends DurableEvent

case class EncodedEvent(metadata: EventMetadata, payload: EventBytes) extends DurableEvent {
  def emitted(localLogId: String, localSequenceNo: Long): EncodedEvent = {
    copy(metadata.copy(
      emitterLogId = localLogId,
      localLogId = localLogId,
      localSequenceNo = localSequenceNo,
      vectorTimestamp = metadata.vectorTimestamp.setLocalTime(localLogId, localSequenceNo)))
  }

  def replicated(localLogId: String, localSequenceNo: Long): EncodedEvent = {
    copy(metadata.copy(
      localLogId = localLogId,
      localSequenceNo = localSequenceNo))
  }
}
