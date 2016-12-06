package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef

import scala.collection.immutable.Seq

object EventsourcingProtocol {
  case class Subscribe(subscriber: ActorRef)

  case class ReplayRead(fromSequenceNr: Long)
  case class ReplayReadSuccess(events: Seq[DecodedEvent])

  case class EmissionWrite(events: Seq[DecodedEvent])
  case class EmissionWriteSuccess(events: Seq[DecodedEvent])
}
