package com.rbmhtechnology.eventuate.sandbox

import akka.actor._
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.stopOnUnserializableKeepOthers
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProcessor.ReplicationProcessResult
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationBlocker.BlockAfter
import com.rbmhtechnology.eventuate.sandbox.ReplicationBlocker.NoBlocker
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer

import scala.collection.immutable.Seq

trait EventLogOps {
  // --- EventLog ---
  private var _sequenceNr: Long = 0L
  private var _versionVector: VectorTime = VectorTime.Zero
  private var _deletionVector: VectorTime = VectorTime.Zero

  var eventStore: Vector[EncodedEvent] = Vector.empty

  def id: String

  def sequenceNr: Long =
    _sequenceNr

  def versionVector: VectorTime =
    _versionVector

  def read(fromSequenceNr: Long): Seq[EncodedEvent] =
    eventStore.drop(fromSequenceNr.toInt - 1)

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

  // --- Eventsourcing ---

  def emissionWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write(events, (evt, snr) => evt.emitted(id, snr))

  // --- Replication ---

  private var progressStore: Map[String, Long] = Map.empty

  def replicationWriteProcessor(sourceLogId: String, currentVersionVector: VectorTime): ReplicationProcessor
  def replicationReadProcessor(targetLogId: String, targetVersionVector: VectorTime, num: Int): ReplicationProcessor

  def replicationRead(fromSequenceNr: Long, num: Int, targetLogId: String, targetVersionVector: VectorTime): ReplicationProcessResult =
    replicationReadProcessor(targetLogId, targetVersionVector, num)
      .apply(read(fromSequenceNr), fromSequenceNr)

  def causalityFilter(versionVector: VectorTime): ReplicationFilter = new ReplicationFilter {
    override def apply(event: EncodedEvent): Boolean = !event.before(versionVector)
  }

  def replicationWrite(events: Seq[EncodedEvent], progress: Long, sourceLogId: String): ReplicationProcessResult = {
    replicationWriteProcessor(sourceLogId, versionVector)
      .apply(events, progress).right.map {
      case (filtered, updatedProgress) => (write(filtered, (evt, snr) => evt.replicated(id, snr)), updatedProgress)
    }
  }

  def progressWrite(progresses: Map[String, Long]): Unit =
    progressStore = progressStore ++ progresses

  def progressRead(logId: String): Long =
    progressStore.getOrElse(logId, 0L)
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

  /* --- Eventsourcing --- */

  private def eventsourcingReceive: Receive = {
    case Subscribe(subscriber) =>
      subscribe(subscriber)
    case Read(from) =>
      val encoded = read(from)
      sender() ! ReadSuccess(decode(encoded))
    case Write(events) =>
      val encoded = emissionWrite(encode(events))
      val decoded = encoded.zip(events).map { case (enc, dec) => dec.copy(enc.metadata) }
      sender() ! WriteSuccess(decoded)
      publish(decoded)
  }

  /* --- Replication --- */

  private def replicationReceive: Receive = {
    case ReplicationRead(from, num, tlid, tvv) =>
      replicationRead(from, num, tlid, tvv) match {
        case Right((processedEvents, progress)) =>
          sender() ! ReplicationReadSuccess(processedEvents, progress)
        case Left(reason) =>
          sender() ! ReplicationReadFailure(new ReplicationStoppedException(reason))
      }
    case ReplicationWrite(events, sourceLogId, progress) =>
      replicationWrite(events, progress, sourceLogId) match {
        case Right((processedEvents, updatedProgress)) =>
          progressWrite(Map(sourceLogId -> updatedProgress))
          val decoded = decode(processedEvents)
          sender() ! ReplicationWriteSuccess(processedEvents, sourceLogId, progress, versionVector)
          publish(decoded)
        case Left(reason) =>
          sender() ! ReplicationWriteFailure(new ReplicationStoppedException(reason))
      }
    case GetReplicationProgressAndVersionVector(logId) =>
      sender() ! GetReplicationProgressAndVersionVectorSuccess(progressRead(logId), versionVector)
  }

  /* --- Replication Filters --- */

  /** Maps target log ids to replication filters used for replication reads */
  private var targetFilters: Map[String, ReplicationFilter] =
    Map.empty

  private def replicationFilterReceive: Receive = {
    case AddTargetFilter(logId, filter) =>
      targetFilters = targetFilters.updated(logId, filter)
  }

  /* --- RFC --- */

  /** Maps target log ids to [[RedundantFilterConfig]]s used to build [[RfcBlocker]]s for replication reads */
  private var redundantFilterConfigs: Map[String, RedundantFilterConfig] =
    Map.empty


  private def rfcReceive: Receive = {
    case AddRedundantFilterConfig(logId, config) =>
      redundantFilterConfigs += logId -> config
  }

  /* --- Scheme evolution --- */

  /** Maps source log ids to [[ReplicationDecider]]s used for replication writes */
  private var eventCompatibilityDeciders: Map[String, ReplicationDecider] =
    Map.empty

  private def schemaEvolutionReceive: Receive = {
    case AddEventCompatibilityDecider(sourceLogId, processor) =>
      eventCompatibilityDeciders += sourceLogId -> processor
    case RemoveEventCompatibilityDecider(sourceLogId) =>
      eventCompatibilityDeciders -= sourceLogId
  }

  override def receive: Receive =
    eventsourcingReceive orElse
    replicationReceive orElse
    replicationFilterReceive orElse
    rfcReceive orElse
    schemaEvolutionReceive

  /* --- Replication processors --- */

  override def replicationWriteProcessor(sourceLogId: String, currentVersionVector: VectorTime) =
    ReplicationProcessor(
      // replication
      ReplicationDecider(causalityFilter(currentVersionVector))
      // schema evolution
      .andThen(eventCompatibilityDeciders.getOrElse(sourceLogId, stopOnUnserializableKeepOthers)))

  override def replicationReadProcessor(targetLogId: String, targetVersionVector: VectorTime, num: Int) = {
    val targetFilter = targetFilters.getOrElse(targetLogId, NoFilter)
    val rfcBlocker = redundantFilterConfigs.get(targetLogId).map(_.rfcBlocker(targetVersionVector)).getOrElse(NoBlocker)
    ReplicationProcessor(
      // replication
      ReplicationDecider(causalityFilter(targetVersionVector))
      // replication filters
      .andThen(ReplicationDecider(targetFilter and sourceFilter))
      // RFC
      .andThen(ReplicationDecider(rfcBlocker))
      // replication
      .andThen(ReplicationDecider(BlockAfter(num))))
  }
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

  class ReplicationStoppedException(reason: BlockReason)
    extends IllegalStateException(s"Replication stopped: $reason")
}
