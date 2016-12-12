package com.rbmhtechnology.eventuate.sandbox

import scala.annotation.tailrec
import scala.collection.immutable.Seq

trait BlockReason
case class MoreThanN(n: Int) extends BlockReason

trait ReplicationBlocker {
  def apply(event: EncodedEvent): Option[BlockReason]
}

object ReplicationBlocker {
  class SequentialReplicationBlocker(blockers: Seq[ReplicationBlocker]) extends ReplicationBlocker {
    override def apply(event: EncodedEvent) = {
      @tailrec
      def go(blockers: Seq[ReplicationBlocker]): Option[BlockReason] =
        blockers match {
          case Nil => None
          case h :: t =>
            h(event) match {
              case None => go(t)
              case reason => reason
            }
        }
      go(blockers)
    }
  }

  object NoBlocker extends ReplicationBlocker {
    override def apply(event: EncodedEvent) = None
  }

  class BlockAfter(n: Int) extends ReplicationBlocker {
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
