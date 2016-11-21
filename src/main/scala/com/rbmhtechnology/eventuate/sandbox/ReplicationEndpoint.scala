package com.rbmhtechnology.eventuate.sandbox

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import akka.actor._
import akka.pattern.{ask, pipe}
import com.rbmhtechnology.eventuate.sandbox.EventLog.AddDecider
import com.rbmhtechnology.eventuate.sandbox.EventLog.RemoveDecider
import com.rbmhtechnology.eventuate.sandbox.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProtocol._
import com.typesafe.config._

import scala.collection.immutable.Seq
import scala.concurrent.Future

object ReplicationEndpoint {
  def logId(endpointId: String, logName: String): String =
    s"${endpointId}_$logName"
}

class ReplicationEndpoint(
  val id: String,
  logNames: Set[String],
  targetFilters: Map[String, ReplicationFilter],
  sourceFilters: Map[String, ReplicationFilter],
  config: Config = ConfigFactory.empty()) {

  import ReplicationEndpoint._

  private val _connections: AtomicReference[Map[String, Set[ActorRef]]] =
    new AtomicReference(Map.empty)

  val system: ActorSystem =
    ActorSystem(s"$id-system", config)

  val settings: ReplicationSettings =
    new ReplicationSettings(system.settings.config)

  val eventLogs: Map[String, ActorRef] =
    logNames.foldLeft(Map.empty[String, ActorRef]) {
      case (logs, logName) => logs + (logName -> createEventLog(logName))
    }

  val connectionAcceptor: ActorRef =
    createConnectionAcceptor()

  import system.dispatcher

  def connections: Set[String] =
    _connections.get.keySet

  def connect(remoteEndpoint: ReplicationEndpoint): Future[String] =
    connect(remoteEndpoint.connectionAcceptor)
  def connect(remoteEndpoint: ReplicationEndpoint, eventReplicationDeciders: Map[String, EventReplicationDecider]): Future[String] =
    connect(remoteEndpoint.connectionAcceptor, eventReplicationDeciders)

  def connect(remoteAcceptor: ActorRef, eventReplicationDeciders: Map[String, EventReplicationDecider] = Map.empty): Future[String] = {
    remoteAcceptor.ask(GetReplicationSourceLogs(logNames))(settings.askTimeout).mapTo[GetReplicationSourceLogsSuccess].map { reply =>
      eventReplicationDeciders.foreach { case (logName, decider) =>
        eventLogs.get(logName).foreach(_ ! AddDecider(logId(reply.endpointId, logName), decider))
      }
      //TODO make sure deciders are added before replicators are started
      val replicators = reply.sourceLogs.map {
        case (logName, sourceLog) =>
          val sourceLogId = logId(reply.endpointId, logName)
          val targetLogId = logId(id, logName)
          createReplicator(sourceLogId, sourceLog, targetLogId, eventLogs(logName))
      }
      addConnection(reply.endpointId, replicators.toSet)
      reply.endpointId
    }
  }

  def disconnect(remoteEndpointId: String): Unit = {
    removeConnection(remoteEndpointId).foreach(system.stop)
    eventLogs.foreach { case (logName, eventLog) =>
      eventLog ! RemoveDecider(logId(remoteEndpointId, logName))
    }
  }

  def terminate(): Future[Terminated] =
    system.terminate()

  private def createConnectionAcceptor(): ActorRef =
    system.actorOf(Props(new ReplicationConnectionAcceptor(id, eventLogs)))

  private def createEventLog(logName: String): ActorRef =
    system.actorOf(EventLog.props(logId(id, logName), targetFilters, sourceFilters.getOrElse(logName, NoFilter)))

  private def createReplicator(sourceLogId: String, sourceLog: ActorRef, targetLogId: String, targetLog: ActorRef): ActorRef =
    system.actorOf(Props(new Replicator(sourceLogId, sourceLog, targetLogId, targetLog)))

  private def addConnection(remoteEndpointId: String, replicators: Set[ActorRef]): Unit = {
    _connections.getAndUpdate(new UnaryOperator[Map[String, Set[ActorRef]]] {
      override def apply(t: Map[String, Set[ActorRef]]): Map[String, Set[ActorRef]] =
        t.updated(remoteEndpointId, replicators)
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
  override def receive = {
    case GetReplicationSourceLogs(targetLogNames) =>
      sender() ! GetReplicationSourceLogsSuccess(endpointId, sourceLogs.filterKeys(targetLogNames.contains))
  }
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

  private def read(fromSequenceNr: Long, targetVersionVector: VectorTime): Unit =
    sourceLog.ask(ReplicationRead(fromSequenceNr, settings.batchSize, targetLogId, targetVersionVector))(settings.askTimeout).pipeTo(self)

  private def write(events: Seq[EncodedEvent], progress: Long): Unit =
    targetLog.ask(ReplicationWrite(events, sourceLogId, progress))(settings.askTimeout).pipeTo(self)

  override def preStart(): Unit =
    fetch()

  override def postStop(): Unit =
    schedule.foreach(_.cancel())
}