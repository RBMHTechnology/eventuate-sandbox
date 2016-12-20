package com.rbmhtechnology.eventuate.sandbox.future

import akka.actor.Scheduler
import akka.pattern.after

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object Retry {
  def apply[T](async: => Future[T], delay: FiniteDuration)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    async recoverWith { case _ => after(delay, s)(apply(async, delay)(ec, s)) }
}

