package com.rbmhtechnology.eventuate.sandbox

import akka.actor._

import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._

import scala.collection.immutable.Seq

trait EventLogOps {
  private var _progressStore: Map[String, Long] = Map.empty
  private var _eventStore: Vector[EncodedEvent] = Vector.empty
  private var _vectorClock: VectorClock = _

  def id: String
  def sourceFilter: ReplicationFilter
  def targetFilter(targetLogId: String): ReplicationFilter

  def vectorClock: VectorClock =
    _vectorClock

  def progressRead(logId: String): Long =
    _progressStore.getOrElse(logId, 0L)

  def replayRead(fromSquenceNr: Long): Seq[EncodedEvent] =
    _eventStore.drop(fromSquenceNr.toInt - 1)

  def replicationRead(fromSequenceNr: Long, num: Int, targetLogId: String, targetVectorTime: VectorTime): Seq[EncodedEvent] =
    replayRead(fromSequenceNr).filter(replicationReadFilter(targetLogId, targetVectorTime).apply).take(num)

  def progressWrite(sourceLogId: String, progress: Long): Unit =
    _progressStore = _progressStore.updated(sourceLogId, progress)

  def emissionWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write(events) {
      case (vcl, evt) =>
        val vcl2 = vcl.incrementLocal
        val evt2 = evt.emitted(id, vcl2.currentLocalTime)
        (vcl2, evt2)
    }

  def replicationWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write(events.filter(causalityFilter(vectorClock.currentTime).apply)) {
      case (vcl, evt) =>
        val vcl2 = vcl.update(evt.metadata.vectorTimestamp)
        val evt2 = evt.replicated(id, vcl2.currentLocalTime)
        (vcl2, evt2)
    }

  private def write(events: Seq[EncodedEvent])(prepare: (VectorClock, EncodedEvent) => (VectorClock, EncodedEvent)): Seq[EncodedEvent] = {
    var res: Vector[EncodedEvent] = Vector.empty
    var log = _eventStore
    var vcl = _vectorClock

    events.foreach { evt =>
      val (c, e) = prepare(vcl, evt)

      res = res :+ e
      log = log :+ e
      vcl = c
    }

    _vectorClock = vcl
    _eventStore = log

    res
  }

  private def replicationReadFilter(targetLogId: String, targetVectorTime: VectorTime): ReplicationFilter =
    causalityFilter(targetVectorTime) and targetFilter(targetLogId) and sourceFilter

  private def causalityFilter(vectorTime: VectorTime): ReplicationFilter = new ReplicationFilter {
    override def apply(event: EncodedEvent): Boolean = !event.before(vectorTime)}

  private[eventuate] def initVectorClock(currentTime: VectorTime): Unit =
    _vectorClock = VectorClock(currentTime, id)
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

  /** Maps target log ids to replication filters */
  private var targetFilters: Map[String, ReplicationFilter] =
    Map.empty

  override def receive = {
    case Subscribe(subscriber) =>
      subscribe(subscriber)
    case Read(from) =>
      val encoded = replayRead(from)
      sender() ! ReadSuccess(decode(encoded))
    case ReplicationRead(from, num, tlid, tvv) =>
      val encoded = replicationRead(from, num, tlid, tvv)
      sender() ! ReplicationReadSuccess(encoded, encoded.lastOption.map(_.metadata.localSequenceNr).getOrElse(from))
    case Write(events) =>
      val encoded = emissionWrite(encode(events))
      val decoded = encoded.zip(events).map { case (enc, dec) => dec.copy(enc.metadata) }
      sender() ! WriteSuccess(decoded)
      publish(decoded)
    case ReplicationWrite(events, sourceLogId, progress) =>
      val encoded = replicationWrite(events); progressWrite(sourceLogId, progress)
      val decoded = decode(encoded)
      sender() ! ReplicationWriteSuccess(encoded, sourceLogId, progress, vectorClock.currentTime)
      publish(decoded)
    case GetReplicationProgressAndVectorTime(logId) =>
      sender() ! GetReplicationProgressAndVectorTimeSuccess(progressRead(logId), vectorClock.currentTime)
    case AddTargetFilter(logId, filter) =>
      targetFilters = targetFilters.updated(logId, filter)
  }

  override def targetFilter(logId: String): ReplicationFilter =
    targetFilters.getOrElse(logId, NoFilter)

  override def preStart(): Unit =
    initVectorClock(VectorTime.Zero)
}

object EventLog {
  def props(id: String): Props =
    props(id, NoFilter)

  def props(id: String, sourceFilter: ReplicationFilter): Props =
    Props(new EventLog(id, sourceFilter))

  def encode(events: Seq[DecodedEvent]): Seq[EncodedEvent] =
    events.map(_.encode)

  def decode(events: Seq[EncodedEvent]): Seq[DecodedEvent] =
    events.map(_.decode)
}
