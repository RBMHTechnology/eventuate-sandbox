package com.rbmhtechnology.eventuate.sandbox

case class VectorClock(currentTime: VectorTime, logId: String) {
  def currentLocalTime: Long =
    currentTime.localTime(logId)

  def update(t: VectorTime): VectorClock =
    copy(currentTime.merge(t)).incrementLocal

  def incrementLocal: VectorClock =
    copy(currentTime.increment(logId))
}
