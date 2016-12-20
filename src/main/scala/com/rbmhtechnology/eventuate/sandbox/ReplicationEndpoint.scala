package com.rbmhtechnology.eventuate.sandbox

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.rbmhtechnology.eventuate.sandbox.EventLog.getLogInfo
import com.rbmhtechnology.eventuate.sandbox.EventsourcingProtocol.Delete
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.logId
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._
import com.rbmhtechnology.eventuate.sandbox.future.Retry
import com.typesafe.config._

import scala.collection.immutable.Seq
import scala.concurrent.Future

object ReplicationEndpoint {
  def logId(endpointId: String, logName: String): String =
    s"${endpointId}_$logName"

  sealed trait ReplicationHistory
  case object CompleteHistory extends ReplicationHistory
  case object CurrentHistory extends ReplicationHistory
  case object NoHistory extends ReplicationHistory
}

class ReplicationEndpoint(
  val id: String,
  logNames: Set[String],
  sourceFilters: Map[String, ReplicationFilter] = Map.empty,
  config: Config = ConfigFactory.empty()) {

  import ReplicationEndpoint._

  private val _connections: AtomicReference[Map[String, Set[ActorRef]]] =
    new AtomicReference(Map.empty)

  val system: ActorSystem =
    ActorSystem(s"$id-system", config.withFallback(ConfigFactory.load()))

  val settings: ReplicationSettings =
    new ReplicationSettings(system.settings.config)

  val eventLogs: Map[String, ActorRef] =
    logNames.foldLeft(Map.empty[String, ActorRef]) {
      case (logs, logName) => logs + (logName -> createEventLog(logName))
    }

  val connectionAcceptor: ActorRef =
    createConnectionAcceptor()

  import system.dispatcher
  implicit private val scheduler = system.scheduler
  implicit private val askTimeout: Timeout = settings.askTimeout

  def connections: Set[String] =
    _connections.get.keySet

  def addTargetFilter(targetEndpointId: String, targetLogName: String, filter: ReplicationFilter): Unit =
    eventLogs(targetLogName) ! AddTargetFilter(logId(targetEndpointId, targetLogName), filter)

  def addRedundantFilterConfig(targetEndpointId: String, config: RedundantFilterConfig): Unit =
    eventLogs(config.logName) ! AddRedundantFilterConfig(logId(targetEndpointId, config.logName), config)

  def connect(remoteEndpoint: ReplicationEndpoint): Future[String] =
    connect(remoteEndpoint.connectionAcceptor)

  def connect(remoteEndpoint: ReplicationEndpoint, history: ReplicationHistory): Future[String] =
    connect(remoteEndpoint.connectionAcceptor, history = history)

  def connect(remoteEndpoint: ReplicationEndpoint, eventCompatibilityDeciders: Map[String, ReplicationDecider]): Future[String] =
    connect(remoteEndpoint.connectionAcceptor, eventCompatibilityDeciders)

  def connect(remoteEndpoint: ReplicationEndpoint, eventCompatibilityDeciders: Map[String, ReplicationDecider], history: ReplicationHistory): Future[String] =
    connect(remoteEndpoint.connectionAcceptor, eventCompatibilityDeciders, history)

  def connect(remoteAcceptor: ActorRef, eventCompatibilityDeciders: Map[String, ReplicationDecider] = Map.empty, history: ReplicationHistory = CompleteHistory): Future[String] = {
    for {
      targetLogInfos <- Future.traverse(eventLogs) { case (logName, logActor) => getLogInfo(logActor).map(logName -> _) }
      connectReply <- remoteAcceptor.ask(Connect(id, targetLogInfos.toMap)).mapTo[ConnectSuccess]
    } yield {
      startReplicatorsAsync(connectReply, eventCompatibilityDeciders, history)
      connectReply.endpointId
    }
  }

  def disconnect(remoteEndpointId: String): Unit = {
    removeConnection(remoteEndpointId).foreach(system.stop)
    eventLogs.foreach { case (logName, eventLog) =>
      eventLog ! RemoveEventCompatibilityDecider(logId(remoteEndpointId, logName))
    }
  }

  def delete(logName: String, toSequenceNo: Long): Unit =
    eventLogs.get(logName).foreach(_ ! Delete(toSequenceNo))

  def terminate(): Future[Terminated] =
    system.terminate()

  private def createConnectionAcceptor(): ActorRef =
    system.actorOf(Props(new ReplicationConnectionAcceptor(id, eventLogs)))

  private def createEventLog(logName: String): ActorRef =
    system.actorOf(EventLog.props(logId(id, logName), sourceFilters.getOrElse(logName, NoFilter)))

  private def startReplicatorsAsync(connectReply: ConnectSuccess, eventCompatibilityDeciders: Map[String, ReplicationDecider], history: ReplicationHistory): Unit = {
    connectReply.sourceLogInfos.foreach {
      case (logName, LogInfo(sourceLog, sourceCvv, sourceDvv)) =>
        val targetLog = eventLogs(logName)
        val sourceLogId = logId(connectReply.endpointId, logName)
        val targetLogId = logId(id, logName)
        eventCompatibilityDeciders.get(logName).foreach(targetLog ! AddEventCompatibilityDecider(sourceLogId, _))
        Retry(startReplicatorAsync(sourceLogId, sourceLog, sourceCvv, sourceDvv, targetLogId, targetLog, history), settings.retryDelay).
          onSuccess { case replicator => addReplicator(connectReply.endpointId, replicator) }
    }
  }

  // TODO Alternatively move logic into Replicator
  private def startReplicatorAsync(sourceLogId: String, sourceLog: ActorRef, sourceCvv: VectorTime, sourceDvv: VectorTime, targetLogId: String, targetLog: ActorRef, history: ReplicationHistory): Future[ActorRef] = {
    if (history == NoHistory) targetLog ! MergeForeignIntoCvvDvv(sourceCvv)
    else if (history == CurrentHistory) targetLog ! MergeForeignIntoCvvDvv(sourceDvv)
    getLogInfo(targetLog)
      .map { info =>
        if (info.currentVersionVector >= sourceDvv || history == CurrentHistory)
          createReplicator(sourceLogId, sourceLog, targetLogId, targetLog)
        else
          throw new IllegalStateException(s"CVV ${info.currentVersionVector} of log $targetLog not >= $sourceDvv of $sourceLogId -> cannot start replication")
      }
  }

  private def createReplicator(sourceLogId: String, sourceLog: ActorRef, targetLogId: String, targetLog: ActorRef): ActorRef =
    system.actorOf(Props(new Replicator(sourceLogId, sourceLog, targetLogId, targetLog)))

  private def addReplicator(remoteEndpointId: String, replicator: ActorRef): Unit = {
    _connections.getAndUpdate(new UnaryOperator[Map[String, Set[ActorRef]]] {
      override def apply(t: Map[String, Set[ActorRef]]): Map[String, Set[ActorRef]] =
        t.updated(remoteEndpointId, t.getOrElse(remoteEndpointId, Set.empty) + replicator)
    })
  }

  private def removeConnection(remoteEndpointId: String): Set[ActorRef] = {
    _connections.getAndUpdate(new UnaryOperator[Map[String, Set[ActorRef]]] {
      override def apply(t: Map[String, Set[ActorRef]]): Map[String, Set[ActorRef]] =
        t - remoteEndpointId
    }).getOrElse(remoteEndpointId, Set())
  }
}

private class ReplicationConnectionAcceptor(endpointId: String, sourceLogs: Map[String, ActorRef]) extends Actor {

  private val settings = new ReplicationSettings(context.system.settings.config)

  import context.dispatcher
  private implicit val askTimeout: Timeout = settings.askTimeout

  override def receive = {
    case Connect(targetEndpointId, targetLogInfos) =>
      Future.traverse(sourceLogs.filterKeys(targetLogInfos.contains)) { case (logName, logActor) =>
        mergeTargetVersionVector(logActor, logId(targetEndpointId, logName), targetLogInfos(logName).currentVersionVector)
          .flatMap(_ => getLogInfo(logActor).map(logName -> _))
      } map { logInfos =>
        ConnectSuccess(endpointId, logInfos.toMap)
      } pipeTo sender()
  }

  private def mergeTargetVersionVector(logActor: ActorRef, logId: String, versionVector: VectorTime): Future[VectorTime] =
    logActor.ask(MergeVersionVector(logId, versionVector))
      .mapTo[MergeVersionVectorSuccess]
      .map(_.updatedVersionVector)

}

object Replicator {
  case object Continue
}

private class Replicator(sourceLogId: String, sourceLog: ActorRef, targetLogId: String, targetLog: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher

  val settings: ReplicationSettings =
    new ReplicationSettings(context.system.settings.config)

  val scheduler: Scheduler =
    context.system.scheduler

  var schedule: Option[Cancellable] =
    None

  val idle: Receive = {
    case Continue =>
      context.become(fetching)
      fetch()
  }

  val fetching: Receive = {
    case GetReplicationProgressAndVersionVectorSuccess(progress, targetVersionVector) =>
      context.become(reading)
      read(progress + 1L, targetVersionVector)
  }

  val reading: Receive = {
    case ReplicationReadSuccess(Seq(), _) =>
      context.become(idle)
      scheduleRead()
    case ReplicationReadSuccess(events, progress) =>
      context.become(writing)
      write(events, progress)
    case ReplicationReadFailure(cause) =>
      context.become(idle)
      scheduleRead()
  }

  val writing: Receive = {
    case ReplicationWriteSuccess(_, _, progress, targetVersionVector) =>
      context.become(reading)
      read(progress + 1L, targetVersionVector)
  }

  override def receive = fetching

  private def scheduleRead(): Unit =
    schedule = Some(scheduler.scheduleOnce(settings.retryDelay, self, Continue))

  private def fetch(): Unit =
    targetLog.ask(GetReplicationProgressAndVersionVector(sourceLogId))(settings.askTimeout).pipeTo(self)

  private def read(fromSequenceNo: Long, targetVersionVector: VectorTime): Unit =
    sourceLog.ask(ReplicationRead(fromSequenceNo, settings.batchSize, targetLogId, targetVersionVector))(settings.askTimeout).pipeTo(self)

  private def write(events: Seq[EncodedEvent], progress: Long): Unit =
    targetLog.ask(ReplicationWrite(events, sourceLogId, progress))(settings.askTimeout).pipeTo(self)

  override def preStart(): Unit =
    fetch()

  override def postStop(): Unit =
    schedule.foreach(_.cancel())
}