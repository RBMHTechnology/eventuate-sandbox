package com.rbmhtechnology.eventuate.sandbox

import akka.actor.ActorRef

import scala.collection.immutable.Seq

object EventsourcingProtocol {
  case class Subscribe(subscriber: ActorRef)

  case class Read(fromSequenceNo: Long)
  case class ReadSuccess(events: Seq[DecodedEvent])

  case class Write(events: Seq[DecodedEvent])
  case class WriteSuccess(events: Seq[DecodedEvent])

  case class Delete(toSequenceNo: Long)
}
