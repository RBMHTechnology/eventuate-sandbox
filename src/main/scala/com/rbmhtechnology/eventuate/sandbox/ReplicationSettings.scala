package com.rbmhtechnology.eventuate.sandbox

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration._

class ReplicationSettings(config: Config) {
  val askTimeout =
    config.getDuration("sandbox.replication.ask-timeout", TimeUnit.MILLISECONDS).millis

  val retryDelay =
    config.getDuration("sandbox.replication.retry-delay", TimeUnit.MILLISECONDS).millis

  val batchSize =
    config.getInt("sandbox.replication.batch-size")
}
