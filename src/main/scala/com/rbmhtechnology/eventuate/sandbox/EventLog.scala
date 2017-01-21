package com.rbmhtechnology.eventuate.sandbox

import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.sandbox.EventCompatibility.stopOnUnserializableKeepOthers
import com.rbmhtechnology.eventuate.sandbox.EventLogWithDeferredDeletion.DeferredDeletionSettings
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProcessor.ReplicationProcessResult
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._
import com.rbmhtechnology.eventuate.sandbox.ReplicationBlocker.BlockAfter
import com.rbmhtechnology.eventuate.sandbox.ReplicationBlocker.NoBlocker
import com.rbmhtechnology.eventuate.sandbox.VectorTime.merge
import com.rbmhtechnology.eventuate.sandbox.serializer.EventPayloadSerializer
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationLong

trait EventLogOps {
  protected def id: String

  protected def versionVector: VectorTime

  protected def deletionVector: VectorTime

  protected def read(fromSequenceNo: Long): Seq[EncodedEvent]

  protected def write(events: Seq[EncodedEvent], prepare: (EncodedEvent, Long) => EncodedEvent): Seq[EncodedEvent]

  protected def deleteWhile(cond: EncodedEvent => Boolean): Long

  protected def mergeForeignIntoCvvDvv(versionVector: VectorTime): Unit
}

trait ProgressStoreOps {

  protected def progressWrite(progresses: Map[String, Long]): Unit

  protected def progressRead(logId: String): Long

  protected def targetVersionVectorWrite(logId: String, versionVector: VectorTime): Unit

  protected def targetVersionVectorRead(logId: String): Option[VectorTime]

  protected def targetVersionVectorReadAll: Map[String, VectorTime]
}

trait InMemoryEventLog extends EventLogOps {
  // Implementation
  private var _sequenceNo: Long = 0L
  private var _versionVector: VectorTime = VectorTime.Zero
  private var _deletionVector: VectorTime = VectorTime.Zero

  private var eventStore: Vector[EncodedEvent] = Vector.empty

  // API
  protected def versionVector: VectorTime =
    _versionVector

  protected def deletionVector: VectorTime =
    _deletionVector

  protected def read(fromSequenceNo: Long): Seq[EncodedEvent] =
    eventStore.dropWhile(_.metadata.localSequenceNo < fromSequenceNo)

  protected def write(events: Seq[EncodedEvent], prepare: (EncodedEvent, Long) => EncodedEvent): Seq[EncodedEvent] = {
    var sno = _sequenceNo
    var cvv = _versionVector
    var log = eventStore

    val written = events.map { event =>
      sno = sno + 1L

      val prepared = prepare(event, sno)

      cvv = cvv.merge(prepared.metadata.vectorTimestamp)
      log = log :+ prepared

      prepared
    }

    _sequenceNo = sno
    _versionVector = cvv
    eventStore = log

    written
  }

  protected def deleteWhile(cond: EncodedEvent => Boolean): Long = {
    val (deleted, updatedEventStore) = eventStore.span(cond)
    eventStore = updatedEventStore
    _deletionVector = _deletionVector.merge(merge(deleted.map(_.metadata.vectorTimestamp)))
    deleted.lastOption.map(_.metadata.localSequenceNo).getOrElse(0)
  }

  protected def mergeForeignIntoCvvDvv(versionVector: VectorTime) = {
    val projectToUnknown = versionVector.projection(_versionVector.value.keySet, negate = true)
    _versionVector = _versionVector.merge(projectToUnknown)
    _deletionVector = _deletionVector.merge(projectToUnknown)
  }
}

trait InMemoryProgressStore extends ProgressStoreOps {
  // Implementation
  private var progressStore: Map[String, Long] = Map.empty

  /** Maps target log ids to respective version vector */
  private var targetVersionVectorStore: Map[String, VectorTime] = Map.empty

  // API
  protected def progressWrite(progresses: Map[String, Long]): Unit =
    progressStore = progressStore ++ progresses

  protected def progressRead(logId: String): Long =
    progressStore.getOrElse(logId, 0L)

  protected def targetVersionVectorWrite(logId: String, versionVector: VectorTime): Unit =
    targetVersionVectorStore += logId -> versionVector

  protected def targetVersionVectorRead(logId: String): Option[VectorTime] =
    targetVersionVectorStore.get(logId)

  override protected def targetVersionVectorReadAll: Map[String, VectorTime] =
    targetVersionVectorStore
}

trait EventLogWithImmediateDeletion extends EventLogOps {
  protected def deleteReceive(): Receive = {
    case Delete(toSequenceNo) =>
      deleteWhile(_.metadata.localSequenceNo <= toSequenceNo)
  }
}

trait EventSubscribers {
  // Implementation
  private var subscribers: Set[ActorRef] = Set.empty

  // API
  protected def subscribe(subscriber: ActorRef): Unit =
    subscribers = subscribers + subscriber

  protected def publish(events: Seq[DecodedEvent]): Unit = for {
    e <- events
    s <- subscribers
  } s ! e

}

trait EventLogWithEventsourcing extends EventLogOps { this: Actor with EventSubscribers =>

  import EventLog._

  // Implementation
  private def emissionWrite(events: Seq[EncodedEvent]): Seq[EncodedEvent] =
    write(events, (evt, sno) => evt.emitted(id, sno))

  private implicit val system = context.system

  // API
  protected def eventsourcingReceive: Receive = {
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

}

trait EventLogWithReplication extends EventLogOps with ProgressStoreOps { this: Actor with EventSubscribers =>

  import EventLogWithReplication._
  import EventLog._

  // Required
  protected def replicationReadDecider(targetLogId: String, targetVersionVector: VectorTime, num: Int): ReplicationDecider
  protected def replicationWriteDecider(sourceLogId: String): ReplicationDecider

  // Implementation
  private implicit val system = context.system

  private def replicationWriteProcessor(sourceLogId: String): ReplicationProcessor =
    ReplicationProcessor(replicationWriteDecider(sourceLogId))
  private def replicationReadProcessor(targetLogId: String, targetVersionVector: VectorTime, num: Int): ReplicationProcessor =
    ReplicationProcessor(replicationReadDecider(targetLogId, targetVersionVector, num) andThen ReplicationDecider(BlockAfter(num)))

  private def replicationRead(fromSequenceNo: Long, num: Int, targetLogId: String, targetVersionVector: VectorTime): ReplicationProcessResult =
    replicationReadProcessor(targetLogId, targetVersionVector, num)
      .apply(read(fromSequenceNo), fromSequenceNo)

  private def causalityFilter(versionVector: VectorTime): ReplicationFilter = new ReplicationFilter {
    override def apply(event: EncodedEvent): Boolean = !event.before(versionVector)
  }

  private def replicationWrite(events: Seq[EncodedEvent], progress: Long, sourceLogId: String): ReplicationProcessResult = {
    replicationWriteProcessor(sourceLogId)
      .apply(events, progress).right.map {
      case (filtered, updatedProgress) => (write(filtered, (evt, sno) => evt.replicated(id, sno)), updatedProgress)
    }
  }

  // API
  protected def replicationReceive: Receive = {
    case ReplicationRead(from, num, tlid, tvv) =>
      targetVersionVectorWrite(tlid, tvv)
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
    case GetLogInfo =>
      sender() ! GetLogInfoSuccess(LogInfo(self, id, versionVector, deletionVector))
    case MergeVersionVector(targetLogId, versionVector) =>
      val merged = versionVector.merge(targetVersionVectorRead(targetLogId).getOrElse(VectorTime.Zero))
      targetVersionVectorWrite(targetLogId, merged)
      sender() ! MergeVersionVectorSuccess(merged)
    case MergeForeignIntoCvvDvv(versionVector) =>
      mergeForeignIntoCvvDvv(versionVector)
  }

  protected def replicationReadCausalityDecider(targetVersionVector: VectorTime): ReplicationDecider =
    ReplicationDecider(causalityFilter(targetVersionVector))

  protected def replicationWriteCausalityDecider: ReplicationDecider =
    ReplicationDecider(causalityFilter(versionVector))
}

object EventLogWithReplication {
  class ReplicationStoppedException(reason: BlockReason)
    extends IllegalStateException(s"Replication stopped: $reason")
}

trait EventLogWithDeferredDeletion { this: Actor with EventLogWithReplication =>

  private val settings: DeferredDeletionSettings =
    new DeferredDeletionSettings(context.system.settings.config)

  private val scheduler = context.system.scheduler

  import context.dispatcher

  private var deletedToSequenceNo: Long = 0L

  protected def deleteReceive(): Receive = {
    case Delete(toSequenceNo) =>
      deletedToSequenceNo = deleteWhile { event =>
        event.metadata.localSequenceNo <= toSequenceNo &&
          targetVersionVectorReadAll.values.forall(event.metadata.vectorTimestamp <= _)
      }
      if(deletedToSequenceNo < toSequenceNo)
        scheduler.scheduleOnce(settings.retryDelay, self, Delete(toSequenceNo))
  }
}

object EventLogWithDeferredDeletion {
  class DeferredDeletionSettings(config: Config) {
    val retryDelay: FiniteDuration =
      config.getDuration("sandbox.log.delete.retry-delay", TimeUnit.MILLISECONDS).millis
  }

}

trait EventLogWithReplicationFilters { this: EventLogWithReplication =>

  // required
  protected def sourceFilter: ReplicationFilter

  // Implementation
  /** Maps target log ids to replication filters used for replication reads */
  private var targetFilters: Map[String, ReplicationFilter] =
    Map.empty

  // API
  protected def replicationFilterReceive: Receive = {
    case AddTargetFilter(logId, filter) =>
      targetFilters = targetFilters.updated(logId, filter)
  }

  protected def replicationReadFilterDecider(targetLogId: String): ReplicationDecider = {
    val targetFilter = targetFilters.getOrElse(targetLogId, NoFilter)
    ReplicationDecider(targetFilter and sourceFilter)
  }
}

trait RedundantFilteredConnections { this: EventLogWithReplicationFilters =>
  // Implementation
  /** Maps target log ids to [[RedundantFilterConfig]]s used to build [[RfcBlocker]]s for replication reads */
  private var redundantFilterConfigs: Map[String, RedundantFilterConfig] =
    Map.empty

  // API
  protected def rfcReceive: Receive = {
    case AddRedundantFilterConfig(logId, config) =>
      redundantFilterConfigs += logId -> config
  }

  protected def replicationReadRfcDecider(targetLogId: String, targetVersionVector: VectorTime) = {
    val rfcBlocker = redundantFilterConfigs.get(targetLogId).map(_.rfcBlocker(targetVersionVector)).getOrElse(NoBlocker)
    ReplicationDecider(rfcBlocker)
  }
}

trait EventLogWithSchemaEvolution { this: Actor with EventLogWithReplication =>
  // Implementation
  /** Maps source log ids to [[ReplicationDecider]]s used for replication writes */
  private var eventCompatibilityDeciders: Map[String, ReplicationDecider] =
    Map.empty

  private implicit val system = context.system

  // API
  protected def schemaEvolutionReceive: Receive = {
    case AddEventCompatibilityDecider(sourceLogId, processor) =>
      eventCompatibilityDeciders += sourceLogId -> processor
    case RemoveEventCompatibilityDecider(sourceLogId) =>
      eventCompatibilityDeciders -= sourceLogId
  }

  protected def replicationWriteSchemaEvolutionDecider(sourceLogId: String) =
    eventCompatibilityDeciders.getOrElse(sourceLogId, stopOnUnserializableKeepOthers)
}

class EventLog(val id: String, val sourceFilter: ReplicationFilter) extends Actor
  with EventLogWithEventsourcing with EventSubscribers
  with EventLogWithDeferredDeletion
  with EventLogWithReplication
  with EventLogWithReplicationFilters with RedundantFilteredConnections
  with EventLogWithSchemaEvolution
  with InMemoryEventLog with InMemoryProgressStore {

  override def receive: Receive =
    eventsourcingReceive orElse
    deleteReceive orElse
    replicationReceive orElse
    replicationFilterReceive orElse
    rfcReceive orElse
    schemaEvolutionReceive

  override def replicationWriteDecider(sourceLogId: String) =
    replicationWriteCausalityDecider
      .andThen(replicationWriteSchemaEvolutionDecider(sourceLogId))

  override def replicationReadDecider(targetLogId: String, targetVersionVector: VectorTime, num: Int) = {
    replicationReadCausalityDecider(targetVersionVector)
      .andThen(replicationReadFilterDecider(targetLogId))
      .andThen(replicationReadRfcDecider(targetLogId, targetVersionVector))
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

  def getLogInfo(logActor: ActorRef)(implicit ec: ExecutionContext, askTimeout: Timeout): Future[LogInfo] =
    logActor.ask(GetLogInfo).mapTo[GetLogInfoSuccess].map(_.logInfo)
}
