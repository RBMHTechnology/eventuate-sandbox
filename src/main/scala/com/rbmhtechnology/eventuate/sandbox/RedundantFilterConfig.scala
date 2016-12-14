package com.rbmhtechnology.eventuate.sandbox

import com.rbmhtechnology.eventuate.sandbox.ReplicationBlocker.NoBlocker
import com.rbmhtechnology.eventuate.sandbox.ReplicationEndpoint.logId

case class RedundantFilterConfig(logName: String, endpointIds: Set[String] = Set.empty, foreign: Boolean = true) {
  def rfcBlocker(targetVersionVector: VectorTime) =
    if(endpointIds.isEmpty) NoBlocker
    else RfcBlocker(targetVersionVector, endpointIds.map(logId(_, logName)), !foreign)
}
