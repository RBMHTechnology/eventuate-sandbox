package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProcessor.ReplicationProcessResult
import com.rbmhtechnology.eventuate.sandbox.ReplicationStopper.NoStopper

import scala.collection.immutable.Seq

object ReplicationProcessor {
  type ReplicationProcessResult = Either[StopReason, (Seq[EncodedEvent], Option[Long])]
}

class ReplicationProcessor(replicationFilter: ReplicationFilter = NoFilter, replicationStopper: ReplicationStopper = NoStopper) {

  def apply(events: Seq[EncodedEvent]): ReplicationProcessResult = {
    var stopReason: Option[StopReason] = None
    var noEventsFiltered = true
    var lastProcessed: Long = 0
    // TODO replace filter by tail recursive function
    val filteredEvents = events.filter { event =>
      stopReason.isEmpty &&
        {
          val passed = replicationFilter(event)
          if(!passed) noEventsFiltered = false
          lastProcessed = event.metadata.localSequenceNr
          passed
        } && {
        stopReason = replicationStopper(event)
        stopReason.isEmpty
      }
    }
    stopReason match {
      case None => Right(filteredEvents, None)
      case Some(reason) if noEventsFiltered && filteredEvents == Nil => Left(reason)
      case _ => Right(filteredEvents, Some(lastProcessed))
    }
  }
}
