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
package com.novus.jdbc

import java.sql.{ Array => SQLArray, CallableStatement, Date, Ref, Blob, Clob, Time, NClob, Timestamp, RowId, SQLXML}
import java.io.Reader
import java.net.URL
import java.util.Calendar
import java.math.BigDecimal
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class StatementResult(callable: CallableStatement){ //doesn't extend CallableStatement so they can make queries in queries

  def getDouble(column: String): Double = checkNaN(callable getDouble (column), java.lang.Double.NaN)
  def getDouble(column: Int): Double = checkNaN(callable getDouble (column), java.lang.Double.NaN)

  def getDouble_?(column: String): Option[Double] = wasNull(callable getDouble (column))
  def getDouble_?(column: Int): Option[Double] = wasNull(callable getDouble (column))

  def getInt_?(column: String): Option[Int] = wasNull(callable getInt (column))
  def getInt_?(column: Int): Option[Int] = wasNull(callable getInt (column))

  def getShort_?(column: String): Option[Short] = wasNull(callable getShort column)
  def getShort_?(column: Int): Option[Short] = wasNull(callable getShort column)

  def getLong_?(column: String): Option[Long] = wasNull(callable getLong (column))
  def getLong_?(column: Int): Option[Long] = wasNull(callable getLong (column))

  def getString_?(column: String): Option[String] = wasNull(callable getString (column))
  def getString_?(column: Int): Option[String] = wasNull(callable getString (column))

  def getFloat(column: String): Float = checkNaN(callable getFloat (column), java.lang.Float.NaN)
  def getFloat(column: Int): Float = checkNaN(callable getFloat (column), java.lang.Float.NaN)

  def getFloat_?(column: String): Option[Float] = wasNull(callable getFloat (column))
  def getFloat_?(column: Int): Option[Float] = wasNull(callable getFloat (column))

  def getDateTime(column: String): DateTime = parseDate(callable getTimestamp column)
  def getDateTime(column: Int): DateTime = parseDate(callable getTimestamp column)

  def getDateTime(column: String, cal: Calendar): DateTime = parseDate(callable getTimestamp (column, cal))
  def getDateTime(column: Int, cal: Calendar): DateTime = parseDate(callable getTimestamp (column, cal))

  protected def parseDate(date: Timestamp) = if(date == null) null else StatementResult parseTimeStamp (date)

  def getDateTime_?(column: String): Option[DateTime] = wasNull(getDateTime (column))
  def getDateTime_?(column: Int): Option[DateTime] = wasNull(getDateTime (column))

  def getDateTime_?(column: String, cal: Calendar): Option[DateTime] = wasNull(getDateTime (column, cal))
  def getDateTime_?(column: Int, cal: Calendar): Option[DateTime] = wasNull(getDateTime (column, cal))

  protected def wasNull[VType](value: VType): Option[VType] = if(callable wasNull ()) None else Option(value)
  protected def checkNaN[VType](value: VType, sub: VType): VType = if(callable wasNull ()) sub else value

  //below here is a decoration

  def getString(parameterIndex: Int) = callable getString (parameterIndex)
  def getBoolean(parameterIndex: Int): Boolean = callable getBoolean (parameterIndex)
  def getByte(parameterIndex: Int): Byte = callable getByte (parameterIndex)
  def getShort(parameterIndex: Int): Short = callable getShort (parameterIndex)
  def getInt(parameterIndex: Int): Int = callable getInt (parameterIndex)
  def getLong(parameterIndex: Int): Long = callable getLong (parameterIndex)
  def getBytes(parameterIndex: Int): Array[Byte] = callable getBytes (parameterIndex)
  def getDate(parameterIndex: Int): Date = callable getDate (parameterIndex)
  def getTime(parameterIndex: Int): Time = callable getTime (parameterIndex)
  def getTimestamp(parameterIndex: Int): Timestamp = callable getTimestamp (parameterIndex)
  def getObject(parameterIndex: Int): AnyRef = callable getObject (parameterIndex)
  def getBigDecimal(parameterIndex: Int): BigDecimal = callable getBigDecimal (parameterIndex)
  def getRef(parameterIndex: Int): Ref = callable getRef (parameterIndex)
  def getBlob(parameterIndex: Int): Blob = callable getBlob (parameterIndex)
  def getClob(parameterIndex: Int): Clob = callable getClob (parameterIndex)
  def getArray(parameterIndex: Int): SQLArray = callable getArray (parameterIndex)
  def getDate(parameterIndex: Int, cal: Calendar): Date = callable getDate (parameterIndex)
  def getTime(parameterIndex: Int, cal: Calendar): Time = callable getTime (parameterIndex)
  def getTimestamp(parameterIndex: Int, cal: Calendar): Timestamp = callable getTimestamp (parameterIndex)
  def getURL(parameterIndex: Int): URL = callable getURL (parameterIndex)
  def getString(parameterName: String): String = callable getString (parameterName)
  def getBoolean(parameterName: String): Boolean = callable getBoolean (parameterName)
  def getByte(parameterName: String): Byte = callable getByte (parameterName)
  def getShort(parameterName: String): Short = callable getShort (parameterName)
  def getInt(parameterName: String): Int = callable getInt (parameterName)
  def getLong(parameterName: String): Long = callable getLong (parameterName)
  def getBytes(parameterName: String): Array[Byte] = callable getBytes (parameterName)
  def getDate(parameterName: String): Date = callable getDate (parameterName)
  def getTime(parameterName: String): Time = callable getTime (parameterName)
  def getTimestamp(parameterName: String): Timestamp = callable getTimestamp (parameterName)
  def getObject(parameterName: String): AnyRef = callable getObject (parameterName)
  def getBigDecimal(parameterName: String): BigDecimal = callable getBigDecimal (parameterName)
  def getRef(parameterName: String): Ref = callable getRef (parameterName)
  def getBlob(parameterName: String): Blob = callable getBlob (parameterName)
  def getClob(parameterName: String): Clob = callable getClob (parameterName)
  def getArray(parameterName: String): SQLArray = callable getArray (parameterName)
  def getDate(parameterName: String, cal: Calendar): Date = callable getDate (parameterName)
  def getTime(parameterName: String, cal: Calendar): Time = callable getTime (parameterName)
  def getTimestamp(parameterName: String, cal: Calendar): Timestamp = callable getTimestamp (parameterName)
  def getURL(parameterName: String): URL = callable getURL (parameterName)
  def getRowId(parameterIndex: Int): RowId = callable getRowId (parameterIndex)
  def getRowId(parameterName: String): RowId = callable getRowId (parameterName)
  def getNClob(parameterIndex: Int): NClob = callable getNClob (parameterIndex)
  def getNClob(parameterName: String): NClob = callable getNClob (parameterName)
  def getSQLXML(parameterIndex: Int): SQLXML = callable getSQLXML (parameterIndex)
  def getSQLXML(parameterName: String): SQLXML = callable getSQLXML (parameterName)
  def getNString(parameterIndex: Int): String = callable getNString (parameterIndex)
  def getNString(parameterName: String): String = callable getNString (parameterName)
  def getNCharacterStream(parameterIndex: Int): Reader = callable getNCharacterStream (parameterIndex)
  def getNCharacterStream(parameterName: String): Reader = callable getNCharacterStream (parameterName)
  def getCharacterStream(parameterIndex: Int): Reader = callable getCharacterStream (parameterIndex)
  def getCharacterStream(parameterName: String): Reader = callable getCharacterStream (parameterName)
}

object StatementResult{
  val pattern = DateTimeFormat forPattern "yyyy-MM-dd hh:mm:ss.fffffffff"

  def parseTimeStamp(time: java.sql.Timestamp) = pattern parseDateTime (time toString)
}