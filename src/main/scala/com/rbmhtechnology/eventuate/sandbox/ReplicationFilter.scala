/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.sandbox

import scala.collection.immutable.Seq

object ReplicationFilter {
  /**
   * Logical AND of given `filters`.
   */
  case class AndFilter(filters: Seq[ReplicationFilter]) extends ReplicationFilter {
    /**
     * Evaluates to `true` if all `filters` evaluate to `true`, `false` otherwise.
     */
    def apply(event: EncodedEvent): Boolean = {
      @annotation.tailrec
      def go(filters: Seq[ReplicationFilter]): Boolean = filters match {
        case Nil     => true
        case f +: fs => if (f(event)) go(fs) else false
      }
      go(filters)
    }
  }

  /**
   * Logical OR of given `filters`.
   */
  case class OrFilter(filters: Seq[ReplicationFilter]) extends ReplicationFilter {
    /**
     * Evaluates to `true` if any of `filters` evaluate to `true`, `false` otherwise.
     */
    def apply(event: EncodedEvent): Boolean = {
      @annotation.tailrec
      def go(filters: Seq[ReplicationFilter]): Boolean = filters match {
        case Nil     => false
        case f +: fs => if (f(event)) true else go(fs)
      }
      go(filters)
    }
  }

  /**
   * Replication filter that evaluates to `true` for all events.
   */
  object NoFilter extends ReplicationFilter {
    /**
     * Evaluates to `true`.
     */
    def apply(event: EncodedEvent): Boolean = true

    /**
     * Returns the given `filter`.
     */
    override def and(filter: ReplicationFilter): ReplicationFilter = filter

    /**
     * Returns this [[NoFilter]].
     */
    override def or(filter: ReplicationFilter): ReplicationFilter = this
  }
}

/**
 * Composable replication filter.
 */
trait ReplicationFilter {
  import ReplicationFilter._

  /**
   * Evaluates this filter on the given `event`.
   */
  def apply(event: EncodedEvent): Boolean

  /**
   * Returns a composed replication filter that represents a logical AND of
   * this filter and the given `filter`.
   */
  def and(filter: ReplicationFilter): ReplicationFilter = this match {
    case f @ AndFilter(filters) => f.copy(filter +: filters)
    case _                      => AndFilter(Seq(filter, this))
  }

  /**
   * Returns a composed replication filter that represents a logical OR of
   * this filter and the given `filter`.
   */
  def or(filter: ReplicationFilter): ReplicationFilter = this match {
    case f @ OrFilter(filters) => f.copy(filter +: filters)
    case _                     => OrFilter(Seq(filter, this))
  }

  /**
   * Combines this filter and the given `filter` by ignoring the given `filter`, i.e. returns `this`
   */
  def leftIdentity(filter: ReplicationFilter): ReplicationFilter = this

  /**
   * Combines this filter and the given `filter` by ignoring this filter, i.e. returns `filter`
   */
  def rightIdentity(filter: ReplicationFilter): ReplicationFilter = filter
}
