package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Block
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Filter
import com.rbmhtechnology.eventuate.sandbox.ReplicationProcessor.ReplicationProcessResult

import scala.annotation.tailrec
import scala.collection.immutable.Seq

object ReplicationProcessor {
  type ReplicationProcessResult = Either[BlockReason, (Seq[EncodedEvent], Long)]
}

case class ReplicationProcessor(replicationDecider: ReplicationDecider) {

  def apply(events: Seq[EncodedEvent], progress: Long): ReplicationProcessResult = {
    var lastProgress: Long = 0

    @tailrec
    def go(in: Seq[EncodedEvent], out: Vector[EncodedEvent]): ReplicationProcessResult = in match {
      case seq if seq.isEmpty =>
        Right(out, progress)
      case seq =>
        replicationDecider(seq.head) match {
          case Block(reason) =>
            Either.cond(lastProgress > 0, (out, lastProgress), reason)
          case decision =>
            lastProgress = seq.head.metadata.localSequenceNo
            go(seq.tail, if(decision == Filter) out else out :+ seq.head)
        }
    }

    go(events, Vector.empty)
  }
}
