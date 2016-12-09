package com.rbmhtechnology.eventuate.sandbox

import akka.actor._
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProcessor.ReplicationProcessResult
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationStopper.StopAfter
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer

import scala.collection.immutable.Seq

trait EventLogOps {
  private var _sequenceNr: Long = 0L
  private var _versionVector: VectorTime = VectorTime.Zero
  private var _deletionVector: VectorTime = VectorTime.Zero

  var eventStore: Vector[EncodedEvent] = Vector.empty
  private var progressStore: Map[String, Long] = Map.empty

  def id: String

  def sourceFilter: ReplicationFilter
  def targetFilter(targetLogId: String): ReplicationFilter

  def sequenceNr: Long =
    _sequenceNr

  def versionVector: VectorTime =
    _versionVector

  def read(fromSquenceNr: Long): Seq[EncodedEvent] =
    eventStore.drop(fromSquenceNr.toInt - 1)

  def causalityFilter(versionVector: VectorTime): ReplicationFilter = new ReplicationFilter {
    override def apply(event: EncodedEvent): Boolean = !event.before(versionVector)
  }

  def replicationReadFilter(targetLogId: String, targetVersionVector: VectorTime): ReplicationFilter =
    causalityFilter(targetVersionVector) and targetFilter(targetLogId) and sourceFilter

  def replicationRead(fromSequenceNr: Long, num: Int, targetLogId: String, targetVersionVector: VectorTime): ReplicationProcessResult =
    new ReplicationProcessor(replicationReadFilter(targetLogId, targetVersionVector), new StopAfter(num)).apply(read(fromSequenceNr))

  def progressRead(logId: String): Long =
    progressStore.getOrElse(logId, 0L)

  def emissionWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write(events, (evt, snr) => evt.emitted(id, snr))

  def replicationWrite(events: Seq[EncodedEvent]): ReplicationProcessResult = {
    new ReplicationProcessor(causalityFilter(_versionVector)).apply(events).right.map {
      case (filtered, progress) => (write(filtered, (evt, snr) => evt.replicated(id, snr)), progress)
    }
  }

  def progressWrite(progresses: Map[String, Long]): Unit =
    progressStore = progressStore ++ progresses

  private def write(events: Seq[EncodedEvent], prepare: (EncodedEvent, Long) => EncodedEvent): Seq[EncodedEvent] = {
    var snr = _sequenceNr
    var cvv = _versionVector
    var log = eventStore

    val written = events.map { event =>
      snr = snr + 1L

      val prepared = prepare(event, snr)

      cvv = cvv.merge(prepared.metadata.vectorTimestamp)
      log = log :+ prepared

      prepared
    }

    _sequenceNr = snr
    _versionVector = cvv
    eventStore = log

    written
  }
}

trait EventSubscribers {
  private var subscribers: Set[ActorRef] = Set.empty

  def subscribe(subscriber: ActorRef): Unit =
    subscribers = subscribers + subscriber

  def publish(events: Seq[DecodedEvent]): Unit = for {
    e <- events
    s <- subscribers
  } s ! e

}

class EventLog(val id: String, val sourceFilter: ReplicationFilter) extends Actor with EventLogOps with EventSubscribers {
  import EventLog._
  import context.system

  /** Maps target log ids to replication filters */
  private var targetFilters: Map[String, ReplicationFilter] =
    Map.empty

  override def receive = {
    case Subscribe(subscriber) =>
      subscribe(subscriber)
    case Read(from) =>
      val encoded = read(from)
      sender() ! ReadSuccess(decode(encoded))
    case ReplicationRead(from, num, tlid, tvv) =>
      replicationRead(from, num, tlid, tvv) match {
        case Right((processedEvents, _)) =>
          sender() ! ReplicationReadSuccess(processedEvents, processedEvents.lastOption.map(_.metadata.localSequenceNr).getOrElse(from))
        case Left(reason) =>
          sender() ! ReplicationReadFailure(new ReplicationStoppedException(reason))
      }
    case Write(events) =>
      val encoded = emissionWrite(encode(events))
      val decoded = encoded.zip(events).map { case (enc, dec) => dec.copy(enc.metadata) }
      sender() ! WriteSuccess(decoded)
      publish(decoded)
    case ReplicationWrite(events, sourceLogId, progress) =>
      replicationWrite(events) match {
        case Right((processedEvents, updatedProgress)) =>
          progressWrite(Map(sourceLogId -> updatedProgress.getOrElse(progress)))
          val decoded = decode(processedEvents)
          sender() ! ReplicationWriteSuccess(processedEvents, sourceLogId, progress, versionVector)
          publish(decoded)
        case Left(reason) =>
          sender() ! ReplicationWriteFailure(new ReplicationStoppedException(reason))
      }
    case GetReplicationProgressAndVersionVector(logId) =>
      sender() ! GetReplicationProgressAndVersionVectorSuccess(progressRead(logId), versionVector)
    case AddTargetFilter(logId, filter) =>
      targetFilters = targetFilters.updated(logId, filter)
  }

  def targetFilter(logId: String): ReplicationFilter =
    targetFilters.getOrElse(logId, NoFilter)
}

object EventLog {
  def props(id: String): Props =
    props(id, NoFilter)

  def props(id: String, sourceFilter: ReplicationFilter): Props =
    Props(new EventLog(id, sourceFilter))

  def encode(events: Seq[DecodedEvent])(implicit system: ActorSystem): Seq[EncodedEvent] =
    events.map(EventPayloadSerializer.encode)

  def decode(events: Seq[EncodedEvent])(implicit system: ActorSystem): Seq[DecodedEvent] =
    events.map(e => EventPayloadSerializer.decode(e).get)

  class ReplicationStoppedException(reason: StopReason)
    extends IllegalStateException(s"Replication stopped: $reason")
}
