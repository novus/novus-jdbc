/*
 * Copyright (c) 2013 Novus Partners, Inc. (http://www.novus.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.novus.jdbc.logging

import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.spi.FilterReply
import ch.qos.logback.classic.spi.ILoggingEvent

/**
 * A configurable Logback Filter object that parses the logging string looking for the time a query took to complete. If
 * no string matches a timed query or the query took longer than the threshold set in the configuration file, a
 * non-passing flag is returned to the logging framework.
 *
 * Most DB implement a slow query log internally. However, this filter can give developers a faster local version to
 * quickly debug problematic queries without having to resort to larger or more powerful system level machinery.
 *
 * If not set in the configuration files, default threshold is 5 seconds.
 */
class SlowQueryFilter extends Filter[ILoggingEvent]{
  private val pattern = """.*timed for (\d+) ms$""".r
  private var cutoffMillis = 5000

  /**
   * Implementation which parses the query string from the logging event, checks against the threshold and makes a
   * determination against the threshold.
   *
   * @param event An object that implements the ILoggingEvent interface.
   */
  def decide(event: ILoggingEvent): FilterReply = parseTime(event getMessage) match{
    case Some(time) if cutoffMillis < time => FilterReply.ACCEPT
    case _ => FilterReply.DENY
  }

  /**
   * Parses the logged message for the time cost in milliseconds of a query.
   *
   * @param message The logged message.
   */
  protected[logging] def parseTime(message: String): Option[Int] = try{
    pattern findFirstMatchIn (message) map (_ group 1) map (_.toInt)
  }
  catch{
    case ex: Exception => None
  }

  /**
   * Allows the cutoff for this Filter to be set in the confirguration file.
   *
   * WARNING: This method works by side-effect.
   *
   * @param cutoff The time in milliseconds which is the boundary of unacceptable.
   */
  def setCutoffMillis(cutoff: Int){
    cutoffMillis = cutoff
  }
}