package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef

import scala.collection.immutable.Seq

object ReplicationProtocol {
  case class AddTargetFilter(targetLogId: String, filter: ReplicationFilter)

  case class GetReplicationSourceLogs(logNames: Set[String])
  case class GetReplicationSourceLogsSuccess(endpointId: String, sourceLogs: Map[String, ActorRef])

  case class GetReplicationProgressAndVectorTime(sourceLogId: String)
  case class GetReplicationProgressAndVectorTimeSuccess(progress: Long, targetVectorTime: VectorTime)

  case class ReplicationRead(fromSequenceNr: Long, num: Int, targetLogId: String, targetVectorTime: VectorTime)
  case class ReplicationReadSuccess(events: Seq[EncodedEvent], progress: Long)

  case class ReplicationWrite(events: Seq[EncodedEvent], sourceLogId: String, progress: Long)
  case class ReplicationWriteSuccess(events: Seq[EncodedEvent], sourceLogId: String, progress: Long, targetVectorTime: VectorTime)
}
