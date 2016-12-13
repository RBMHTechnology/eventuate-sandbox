package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.RfcBlocker.RfcConditionViolated

object RfcBlocker {
  case class RfcConditionViolated(eventTime: VectorTime, targetVersionVector: VectorTime, projectionProcessIds: Set[String], negateProjection: Boolean) extends BlockReason
}

case class RfcBlocker(targetVersionVector: VectorTime, processIds: Set[String], negateProjection: Boolean) extends ReplicationBlocker {
  def apply(event: EncodedEvent): Option[BlockReason] =
    if(event.metadata.vectorTimestamp.projection(processIds, negateProjection) <= targetVersionVector)
      None
    else
      Some(RfcConditionViolated(event.metadata.vectorTimestamp, targetVersionVector, processIds, negateProjection))
}
