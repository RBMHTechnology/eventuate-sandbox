package com.rbmhtechnology.eventuate.sandbox

import akka.actor._
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.eventCompatibility
import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.ReplicationDecision.replicationContinues
import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.ReplicationDecision.unwrapKeep
import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.Filter
import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.ReplicationDecision
import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.Stop
import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.StopOnUnserializableKeepOthers
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer

import scala.collection.immutable.Seq

trait EventLogOps {
  private var _sequenceNr: Long = 0L
  private var _versionVector: VectorTime = VectorTime.Zero
  private var _deletionVector: VectorTime = VectorTime.Zero

  var eventStore: Vector[EncodedEvent] = Vector.empty
  private var progressStore: Map[String, Long] = Map.empty

  def id: String

  def targetFilter(targetLogId: String): ReplicationFilter
  def sourceFilter: ReplicationFilter
  def eventCompatibilityFilter: EventReplicationDecider

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

  def replicationRead(fromSequenceNr: Long, num: Int, targetLogId: String, targetVersionVector: VectorTime): Seq[EncodedEvent] =
    read(fromSequenceNr).filter(replicationReadFilter(targetLogId, targetVersionVector).apply).take(num)

  def progressRead(logId: String): Long =
    progressStore.getOrElse(logId, 0L)

  def emissionWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write[EncodedEvent](events, identity, (evt, snr) => evt.emitted(id, snr))

  def replicationWrite(events: Seq[FullEvent]): Seq[FullEvent] =
    write[FullEvent](
      events.filter(ev => causalityFilter(_versionVector)(ev.encoded)),
      _.encoded,
      (evt, snr) => {
        val encoded = evt.encoded.replicated(id, snr)
        evt.copy(encoded, evt.decoded.copy(metadata = encoded.metadata))
      })

  def progressWrite(progresses: Map[String, Long]): Unit =
    progressStore = progressStore ++ progresses

  private def write[A](as: Seq[A], getEncoded: A => EncodedEvent, prepare: (A, Long) => A): Seq[A] = {
    var snr = _sequenceNr
    var cvv = _versionVector
    var log = eventStore

    val written = as.map { event =>
      snr = snr + 1L

      val prepared = prepare(event, snr)

      cvv = cvv.merge(getEncoded(prepared).metadata.vectorTimestamp)
      log = log :+ getEncoded(prepared)

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

class EventLog(val id: String, val targetFilters: Map[String, ReplicationFilter], val sourceFilter: ReplicationFilter, val eventCompatibilityFilter: EventReplicationDecider) extends Actor with EventLogOps with EventSubscribers {
  import EventLog._

  private implicit val serialization =
    SerializationExtension(context.system)

  override def receive = {
    case Subscribe(subscriber) =>
      subscribe(subscriber)
    case Read(from) =>
      val encoded = read(from)
      sender() ! ReadSuccess(decode(encoded))
    case ReplicationRead(from, num, tlid, tvv) =>
      val encoded = replicationRead(from, num, tlid, tvv)
      sender() ! ReplicationReadSuccess(encoded, encoded.lastOption.map(_.metadata.localSequenceNr).getOrElse(from))
    case Write(events) =>
      val encoded = emissionWrite(encode(events))
      val decoded = encoded.zip(events).map { case (enc, dec) => dec.copy(enc.metadata) }
      sender() ! WriteSuccess(decoded)
      publish(decoded)
    case ReplicationWrite(encodedEvents, sourceLogId, progress) =>
      val (continued, stopped) = split(encodedEvents, sourceLogId)
      val filtered = continued
        .filter(_ != Filter)
        .map(unwrapKeep)

      val events = replicationWrite(filtered.toList)
      if(stopped.isEmpty) progressWrite(Map(sourceLogId -> progress))
      val response =
        failureIfStoppedOnFirst(continued ++ stopped)
        .getOrElse(ReplicationWriteSuccess(events.map(_.encoded), sourceLogId, progress, versionVector))
      sender() ! response
      publish(events.map(_.decoded))
    case GetReplicationProgressAndVersionVector(logId) =>
      sender() ! GetReplicationProgressAndVersionVectorSuccess(progressRead(logId), versionVector)
  }

  private def split(encodedEvents: Seq[EncodedEvent], sourceLogId: String): (Stream[ReplicationDecision], Stream[ReplicationDecision]) = {
    Stream(encodedEvents: _*)
      .map(eventCompatibility)
      .map(eventCompatibilityFilter.decide(sourceLogId))
      .span(replicationContinues)
  }

  def targetFilter(targetLogId: String): ReplicationFilter =
    targetFilters.getOrElse(targetLogId, NoFilter)

  private def failureIfStoppedOnFirst(decisions: Stream[ReplicationDecision]): Option[ReplicationWriteFailure] =
    decisions.headOption.collect {
      case Stop(reason) => ReplicationWriteFailure(new ReplicationStoppedException(reason))
    }
}

object EventLog {
  def props(id: String): Props =
    props(id, Map.empty, NoFilter, StopOnUnserializableKeepOthers)

  def props(id: String, targetFilters: Map[String, ReplicationFilter], sourceFilter: ReplicationFilter, eventCompatibilityFilter: EventReplicationDecider): Props =
    Props(new EventLog(id, targetFilters, sourceFilter, eventCompatibilityFilter))

  def encode(events: Seq[DecodedEvent])(implicit serialization: Serialization): Seq[EncodedEvent] =
    events.map(EventPayloadSerializer.encode)

  def decode(events: Seq[EncodedEvent])(implicit serialization: Serialization): Seq[DecodedEvent] =
    events.map(e => EventPayloadSerializer.decode(e).get)

  class ReplicationStoppedException(compatibility: EventCompatibility) extends IllegalStateException(s"Replication stooped: $compatibility")
}
