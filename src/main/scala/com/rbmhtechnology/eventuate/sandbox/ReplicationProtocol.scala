package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef

import scala.collection.immutable.Seq

object ReplicationProtocol {
  case class AddEventCompatibilityDecider(sourceLogId: String, decider: ReplicationDecider)
  case class RemoveEventCompatibilityDecider(sourceLogId: String)

  case class AddTargetFilter(targetLogId: String, filter: ReplicationFilter)

  case class AddRedundantFilterConfig(targetLogId: String, config: RedundantFilterConfig)

  case class LogInfo(logActor: ActorRef, logId: String, currentVersionVector: VectorTime, deletionVersionVector: VectorTime)

  /**
    * Sent by a location to remote locations to begin replicating events from them.
    *
    * @param targetLogInfos maps log-names to [[LogInfo]] of logs that shall be connected to
    *                       remote logs to replicate events from them. These are local logs for
    *                       the sending side and remote logs for the receiving side (of this message).
    */
  case class Connect(endpointId: String, targetLogInfos: Map[String, LogInfo])
  /**
    * Sent by a location in reply to a [[Connect]] message.
    *
    * @param sourceLogInfos maps log-names to [[LogInfo]] of logs that are source logs for the
    *                       target logs referenced by the [[Connect]] message.
    *                       These are local logs for
    *                       the sending side and remote logs for the receiving side (of this message).
    */
  case class ConnectSuccess(endpointId: String, sourceLogInfos: Map[String, LogInfo])

  case class GetReplicationProgressAndVersionVector(sourceLogId: String)
  case class GetReplicationProgressAndVersionVectorSuccess(progress: Long, targetVersionVector: VectorTime)

  case object GetLogInfo
  case class GetLogInfoSuccess(logInfo: LogInfo)

  case class MergeVersionVector(targetLogId: String, versionVector: VectorTime)
  case class MergeVersionVectorSuccess(updatedVersionVector: VectorTime)

  /**
    * Merge the foreign parts of `versionVector` into the local current and deletion version vector.
    *
    * An entry in `versionVector` is considered foreign if there is no entry in the local current
    * version vector for its process id, i.e. the location has not yet seen an event from this
    * process id.
    */
  case class MergeForeignIntoCvvDvv(versionVector: VectorTime)

  case class ReplicationRead(fromSequenceNo: Long, num: Int, targetLogId: String, targetVersionVector: VectorTime)
  case class ReplicationReadSuccess(events: Seq[EncodedEvent], progress: Long)
  case class ReplicationReadFailure(cause: Throwable)

  case class ReplicationWrite(events: Seq[EncodedEvent], sourceLogId: String, progress: Long)
  case class ReplicationWriteSuccess(events: Seq[EncodedEvent], sourceLogId: String, progress: Long, targetVersionVector: VectorTime)
  case class ReplicationWriteFailure(cause: Throwable)
}
