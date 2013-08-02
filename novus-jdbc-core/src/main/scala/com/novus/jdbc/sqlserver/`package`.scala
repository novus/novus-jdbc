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
package com.novus.jdbc.sqlserver

import com.novus.jdbc.{StatementResult, RichResultSet, Queryable}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import java.util.Calendar
import java.sql.{CallableStatement, ResultSet}

object `package` extends SqlServerImplicits

sealed trait SqlServer

trait SqlServerImplicits {
  //TODO: Add in config so US/Eastern can, at some point, be able to be substituted.
  implicit object SqlServerQueryable extends Queryable[SqlServer]{
    val pattern = DateTimeFormat forPattern "yyyy-MM-dd HH:mm:ss.SSS"
    val formatter = pattern withZone timezone()

    def timezone(id: String = "US/Eastern"): DateTimeZone = DateTimeZone forID (id)
    def timezone(cal: Calendar) = DateTimeZone forTimeZone (cal getTimeZone)

    override def wrap(row: ResultSet) = new RichResultSet(row){
      override def getDateTime(column: String): DateTime = parseDate(row getString column)
      override def getDateTime(column: Int): DateTime = parseDate(row getString column)

      protected def parseDate(date: String) = if(date == null || date.isEmpty) null else formatter parseDateTime date

      override def getDateTime(column: String, cal: Calendar): DateTime = parseDate(row getString column, cal)
      override def getDateTime(column: Int, cal: Calendar): DateTime = parseDate(row getString column, cal)

      protected def parseDate(date: String, cal: Calendar) =
        if(date == null || date.isEmpty) null else pattern withZone timezone(cal) parseDateTime (date)
    }

    override def wrap(callable: CallableStatement) = new StatementResult(callable){
      override def getDateTime(column: String): DateTime = parseDate(getString(column))
      override def getDateTime(column: Int): DateTime = parseDate(getString(column))

      protected def parseDate(date: String) = if(date == null || date.isEmpty) null else formatter parseDateTime date

      override def getDateTime(column: String, cal: Calendar): DateTime = parseDate(getString(column), cal)
      override def getDateTime(column: Int, cal: Calendar): DateTime = parseDate(getString(column), cal)

      protected def parseDate(date: String, cal: Calendar) =
        if(date == null || date.isEmpty) null else pattern withZone timezone(cal) parseDateTime (date)
    }
  }
}