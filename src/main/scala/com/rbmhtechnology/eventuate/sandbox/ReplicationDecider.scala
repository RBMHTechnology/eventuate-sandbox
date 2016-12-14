package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Continue
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.ReplicationDecision

object ReplicationDecider {
  sealed trait ReplicationDecision
  case object Filter extends ReplicationDecision
  case class Block(reason: BlockReason) extends ReplicationDecision
  case object Continue extends ReplicationDecision

  def apply(filter: ReplicationFilter): ReplicationDecider = new ReplicationDecider {
    override def apply(event: EncodedEvent) =
      if (filter(event)) Continue else Filter
  }

  def apply(blocker:  ReplicationBlocker): ReplicationDecider = new ReplicationDecider {
    override def apply(event: EncodedEvent) =
      blocker(event).map(Block).getOrElse(Continue)
  }
}

trait ReplicationDecider { outer =>
  def apply(event: EncodedEvent): ReplicationDecision

  def andThen(nextDecider: ReplicationDecider) = new ReplicationDecider {
    override def apply(event: EncodedEvent) = outer(event) match {
      case Continue => nextDecider(event)
      case result => result
    }
  }
}
