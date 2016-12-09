package com.rbmhtechnology.eventuate.sandbox

import scala.annotation.tailrec
import scala.collection.immutable.Seq

sealed trait StopReason
case class MoreThanN(n: Int) extends StopReason

trait ReplicationStopper {
  def apply(event: EncodedEvent): Option[StopReason]
}

object ReplicationStopper {
  class SequentialReplicationStopper(stoppers: Seq[ReplicationStopper]) extends ReplicationStopper {
    override def apply(event: EncodedEvent) = {
      @tailrec
      def go(stoppers: Seq[ReplicationStopper]): Option[StopReason] =
        stoppers match {
          case Nil => None
          case h :: t =>
            h(event) match {
              case None => go(t)
              case reason => reason
            }
        }
      go(stoppers)
    }
  }

  object NoStopper extends ReplicationStopper {
    override def apply(event: EncodedEvent) = None
  }

  class StopAfter(n: Int) extends ReplicationStopper {
    private var count: Int = 0
    override def apply(event: EncodedEvent) =
      if(count > n)
        Some(MoreThanN(n))
      else {
        count += 1
        None
      }
  }
}
