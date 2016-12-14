package com.rbmhtechnology.eventuate.sandbox

import scala.annotation.tailrec
import scala.collection.immutable.Seq

trait BlockReason
case class MoreThanN(n: Int) extends BlockReason

trait ReplicationBlocker {
  def apply(event: EncodedEvent): Option[BlockReason]
}

object ReplicationBlocker {
  case class SequentialReplicationBlocker(blockers: Seq[ReplicationBlocker]) extends ReplicationBlocker {
    override def apply(event: EncodedEvent) = {
      @tailrec
      def go(blockers: Seq[ReplicationBlocker]): Option[BlockReason] = blockers match {
        case Nil => None
        case h :: t =>
          val reason = h(event)
          if(reason.isDefined) reason else go(t) // getOrElse violates tailrec
      }
      go(blockers)
    }
  }

  object NoBlocker extends ReplicationBlocker {
    override def apply(event: EncodedEvent) = None
  }

  case class BlockAfter(n: Int) extends ReplicationBlocker {
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
