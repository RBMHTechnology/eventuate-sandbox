package com.rbmhtechnology.eventuate.sandbox

import akka.actor._

import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._

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
    write(events, (evt, snr) => evt.emitted(id, snr))

  def replicationWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write(events.filter(causalityFilter(_versionVector).apply), (evt, snr) => evt.replicated(id, snr))

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

class EventLog(val id: String, val targetFilters: Map[String, ReplicationFilter], val sourceFilter: ReplicationFilter) extends Actor with EventLogOps with EventSubscribers {
  import EventLog._

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
    case ReplicationWrite(events, progresses) =>
      val encoded = replicationWrite(events); progressWrite(progresses)
      val decoded = decode(encoded)
      sender() ! ReplicationWriteSuccess(encoded, progresses, versionVector)
      publish(decoded)
    case GetReplicationProgressAndVersionVector(logId) =>
      sender() ! GetReplicationProgressAndVersionVectorSuccess(progressRead(logId), versionVector)
  }

  def targetFilter(targetLogId: String): ReplicationFilter =
    targetFilters.getOrElse(targetLogId, NoFilter)
}

object EventLog {
  def props(id: String): Props =
    props(id, Map.empty, NoFilter)

  def props(id: String, targetFilters: Map[String, ReplicationFilter], sourceFilter: ReplicationFilter): Props =
    Props(new EventLog(id, targetFilters, sourceFilter))

  def encode(events: Seq[DecodedEvent]): Seq[EncodedEvent] =
    events.map(_.encode)

  def decode(events: Seq[EncodedEvent]): Seq[DecodedEvent] =
    events.map(_.decode)
}
