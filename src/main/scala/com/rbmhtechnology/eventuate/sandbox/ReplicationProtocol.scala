package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef

import scala.collection.immutable.Seq

object ReplicationProtocol {
  case class GetReplicationSourceLogs(logNames: Set[String])
  case class GetReplicationSourceLogsSuccess(endpointId: String, sourceLogs: Map[String, ActorRef])

  case class GetReplicationProgressAndVersionVector(sourceLogId: String)
  case class GetReplicationProgressAndVersionVectorSuccess(progress: Long, targetVersionVector: VectorTime)

  case class ReplicationRead(fromSequenceNr: Long, num: Int, targetLogId: String, targetVersionVector: VectorTime)
  case class ReplicationReadSuccess(events: Seq[EncodedEvent], progress: Long)

  case class ReplicationWrite(events: Seq[EncodedEvent], progresses: Map[String, Long])
  case class ReplicationWriteSuccess(events: Seq[EncodedEvent], progresses: Map[String, Long], targetVersionVector: VectorTime)
}
