package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.EventReplicationDecider.ReplicationDecision

// TODO find better names
trait EventReplicationDecider {
  def decide(sourceLogId: String)(eventCompatibility: EventCompatibility): ReplicationDecision
}

object EventReplicationDecider {

  trait ReplicationDecision
  case class Keep(event: FullEvent) extends ReplicationDecision
  case class Stop(reason: EventCompatibility) extends ReplicationDecision
  case object Filter extends ReplicationDecision

  object ReplicationDecision {
    def unwrapKeep(decision: ReplicationDecision): FullEvent = decision match {
      case Keep(event) => event
    }
    def replicationContinues(decision: ReplicationDecision): Boolean =
      !decision.isInstanceOf[Stop]
  }

  object StopOnIncompatibility extends EventReplicationDecider {
    override def decide(sourceLogId: String)(eventCompatibility: EventCompatibility): ReplicationDecision =
      eventCompatibility match {
        case Compatible(event) => Keep(event)
        case _ => Stop(eventCompatibility)
      }
  }

  object StopOnUnserializableKeepOthers extends EventReplicationDecider {
    override def decide(sourceLogId: String)(eventCompatibility: EventCompatibility): ReplicationDecision =
      eventCompatibility match {
        case Compatible(event) => Keep(event)
        case MinorIncompatibility(event, _, _) => Keep(event)
        case NoEventVersion(event, _) => Keep(event)
        case NotEventPayloadSerializer(event, _) => Keep(event)
        case _ => Stop(eventCompatibility)
      }
  }
}

