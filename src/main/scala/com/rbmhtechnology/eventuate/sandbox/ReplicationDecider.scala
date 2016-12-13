package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.ReplicationBlocker.NoBlocker
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.Continue
import com.rbmhtechnology.eventuate.sandbox.ReplicationDecider.ReplicationDecision

object ReplicationDecider {
  sealed trait ReplicationDecision
  case object Filter extends ReplicationDecision
  case class Block(reason: BlockReason) extends ReplicationDecision
  case object Continue extends ReplicationDecision

  def apply(replicationFilter: ReplicationFilter, replicationBlocker:  ReplicationBlocker = NoBlocker): ReplicationDecider = new ReplicationDecider {
    override def apply(event: EncodedEvent) =
      if (replicationFilter(event)) replicationBlocker(event).map(Block).getOrElse(Continue) else Filter
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
