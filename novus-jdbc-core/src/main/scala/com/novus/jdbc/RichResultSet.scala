
package com.novus.jdbc

import java.sql.{ResultSet, Date, Time, Timestamp, SQLWarning, ResultSetMetaData, Statement, Ref, RowId, Blob, Clob, NClob, SQLXML}
import org.joda.time.DateTime
import java.io.{InputStream, Reader}
import java.net.URL
import java.util.{Calendar, Map => JMap}
import java.math.BigDecimal
import org.joda.time.format.DateTimeFormat

trait ResultSetWrapper[DBType]{
  def wrap(row: ResultSet): RichResultSet
}

//This is where a Scala 2.10 Value Type would come in handy.
abstract class RichResultSet(row: ResultSet) extends ResultSet{
  def getDouble(column: String): Double = checkNaN(row getDouble (column), java.lang.Double.NaN)
  def getDouble(column: Int): Double = checkNaN(row getDouble (column), java.lang.Double.NaN)

  def getDouble_?(column: String): Option[Double] = wasNull(row getDouble (column))
  def getDouble_?(column: Int): Option[Double] = wasNull(row getDouble (column))

  def getInt_?(column: String): Option[Int] = wasNull(row getInt (column))
  def getInt_?(column: Int): Option[Int] = wasNull(row getInt (column))

  def getLong_?(column: String): Option[Long] = wasNull(row getLong (column))
  def getLong_?(column: Int): Option[Long] = wasNull(row getLong (column))

  def getString_?(column: String): Option[String] = wasNull(row getString (column))
  def getString_?(column: Int): Option[String] = wasNull(row getString (column))

  def getFloat(column: String): Float = checkNaN(row getFloat (column), java.lang.Float.NaN)
  def getFloat(column: Int): Float = checkNaN(row getFloat (column), java.lang.Float.NaN)

  def getFloat_?(column: String): Option[Float] = wasNull(row getFloat (column))
  def getFloat_?(column: Int): Option[Float] = wasNull(row getFloat (column))

  def getDateTime(column: String): DateTime ={
    val date = row getTimestamp (column)

    if(date == null) null else RichResultSet parseTimeStamp (date)
  }

  def getDateTime(column: Int): DateTime ={
    val date = row getTimestamp (column)

    if(date == null) null else RichResultSet parseTimeStamp (date)
  }

  def getDateTime(column: String, cal: Calendar): DateTime ={
    val date = row getTimestamp (column, cal)

    if(date == null) null else RichResultSet parseTimeStamp (date)
  }
  def getDateTime(column: Int, cal: Calendar): DateTime ={
    val date = row getTimestamp (column, cal)

    if(date == null) null else RichResultSet parseTimeStamp (date)
  }

  def getDateTime_?(column: String): Option[DateTime] = wasNull(getDateTime (column))
  def getDateTime_?(column: Int): Option[DateTime] = wasNull(getDateTime (column))

  def getDateTime_?(column: String, cal: Calendar): Option[DateTime] = wasNull(getDateTime (column, cal))
  def getDateTime_?(column: Int, cal: Calendar): Option[DateTime] = wasNull(getDateTime (column, cal))

  protected def wasNull[VType](value: VType): Option[VType] = if(row wasNull ()) None else Option(value)
  protected def checkNaN[VType](value: VType, sub: VType): VType = if(row wasNull ()) sub else value

  //Everything below this point is just decorated functions.

  def next(): Boolean = row next()

  def close() {
    row close ()
  }

  def wasNull(): Boolean = row wasNull ()
  def getString(columnIndex: Int) = row getString (columnIndex)
  def getBoolean(columnIndex: Int) = row getBoolean (columnIndex)
  def getByte(columnIndex: Int) = row getByte (columnIndex)
  def getShort(columnIndex: Int) = row getShort (columnIndex)
  def getInt(columnIndex: Int) = row getInt (columnIndex)
  def getLong(columnIndex: Int) = row getLong (columnIndex)
  def getBigDecimal(columnIndex: Int, scale: Int) = row getBigDecimal (columnIndex)
  def getBytes(columnIndex: Int) = row getBytes (columnIndex)
  def getDate(columnIndex: Int) = row getDate (columnIndex)
  def getTime(columnIndex: Int) = getTime (columnIndex)
  def getTimestamp(columnIndex: Int): Timestamp = row getTimestamp (columnIndex)
  def getAsciiStream(columnIndex: Int): InputStream = row getAsciiStream (columnIndex)
  def getUnicodeStream(columnIndex: Int): InputStream = row getUnicodeStream (columnIndex)
  def getBinaryStream(columnIndex: Int): InputStream = row getBinaryStream (columnIndex)
  def getString(columnLabel: String): String = row getString(columnLabel)
  def getBoolean(columnLabel: String): Boolean = row getBoolean (columnLabel)
  def getByte(columnLabel: String): Byte = row getByte (columnLabel)
  def getShort(columnLabel: String): Short = row getShort (columnLabel)
  def getInt(columnLabel: String): Int = row getInt (columnLabel)
  def getLong(columnLabel: String): Long = row getLong (columnLabel)
  def getBigDecimal(columnLabel: String, scale: Int): BigDecimal = row getBigDecimal (columnLabel)
  def getBytes(columnLabel: String) = row getBytes (columnLabel)
  def getDate(columnLabel: String) = row getDate (columnLabel)
  def getTime(columnLabel: String) = row getTime (columnLabel)
  def getTimestamp(columnLabel: String): Timestamp = row getTimestamp (columnLabel)
  def getAsciiStream(columnLabel: String): InputStream = row getAsciiStream (columnLabel)
  @deprecated def getUnicodeStream(columnLabel: String): InputStream = row getUnicodeStream (columnLabel)
  def getBinaryStream(columnLabel: String): InputStream = row getBinaryStream (columnLabel)
  def getWarnings: SQLWarning = row getWarnings

  def clearWarnings() {
    row clearWarnings ()
  }

  def getCursorName: String = row getCursorName
  def getMetaData: ResultSetMetaData = row getMetaData
  def getObject(columnIndex: Int): AnyRef = row getObject (columnIndex)
  def getObject(columnLabel: String): AnyRef = row getObject (columnLabel)
  def findColumn(columnLabel: String): Int = row findColumn (columnLabel)
  def getCharacterStream(columnIndex: Int): Reader = row getCharacterStream (columnIndex)
  def getCharacterStream(columnLabel: String): Reader = row getCharacterStream (columnLabel)
  def getBigDecimal(columnIndex: Int): BigDecimal = row getBigDecimal (columnIndex)
  def getBigDecimal(columnLabel: String): BigDecimal = row getBigDecimal (columnLabel)
  def isBeforeFirst: Boolean = row isBeforeFirst
  def isAfterLast: Boolean = row isAfterLast
  def isFirst: Boolean = row isFirst
  def isLast: Boolean = row isLast

  def beforeFirst() {
    row beforeFirst ()
  }

  def afterLast() {
    row afterLast ()
  }

  def first(): Boolean = row first ()
  def last(): Boolean = row last ()
  def getRow: Int = row getRow
  def absolute(rrow: Int): Boolean = row absolute (rrow)
  def relative(rows: Int): Boolean = row relative (rows)

  def previous(): Boolean = row previous ()

  def setFetchDirection(direction: Int) {
    row setFetchDirection (direction)
  }

  def getFetchDirection: Int = row getFetchDirection

  def setFetchSize(rows: Int) {
    row setFetchSize (rows)
  }

  def getFetchSize: Int = row getFetchSize
  def getType: Int = row getType
  def getConcurrency: Int = row getConcurrency
  def rowUpdated(): Boolean = row rowUpdated ()
  def rowInserted(): Boolean = row rowInserted ()
  def rowDeleted(): Boolean = row rowDeleted ()

  def updateNull(columnIndex: Int) {
    row updateNull (columnIndex)
  }

  def updateBoolean(columnIndex: Int, x: Boolean) {
    row updateBoolean (columnIndex, x)
  }

  def updateByte(columnIndex: Int, x: Byte) {
    row updateByte (columnIndex, x)
  }

  def updateShort(columnIndex: Int, x: Short) {
    row updateShort (columnIndex, x)
  }

  def updateInt(columnIndex: Int, x: Int) {
    row updateInt (columnIndex, x)
  }

  def updateLong(columnIndex: Int, x: Long) {
    row updateLong (columnIndex, x)
  }

  def updateFloat(columnIndex: Int, x: Float) {
    row updateFloat (columnIndex, x)
  }

  def updateDouble(columnIndex: Int, x: Double) {
    row updateDouble (columnIndex, x)
  }

  def updateBigDecimal(columnIndex: Int, x: BigDecimal) {
    row updateBigDecimal (columnIndex, x)
  }

  def updateString(columnIndex: Int, x: String) {
    row updateString (columnIndex, x)
  }

  def updateBytes(columnIndex: Int, x: Array[Byte]) {
    row updateBytes (columnIndex, x)
  }

  def updateDate(columnIndex: Int, x: Date) {
    row updateDate (columnIndex, x)
  }

  def updateTime(columnIndex: Int, x: Time) {
    row updateTime (columnIndex, x)
  }

  def updateTimestamp(columnIndex: Int, x: Timestamp) {
    row updateTimestamp (columnIndex, x)
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int) {
    row updateAsciiStream (columnIndex, x)
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int) {
    row updateBinaryStream (columnIndex, x, length)
  }

  def updateCharacterStream(columnIndex: Int, x: Reader, length: Int) {
    row updateCharacterStream (columnIndex, x, length)
  }

  def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int) {
    row updateObject (columnIndex, x, scaleOrLength)
  }

  def updateObject(columnIndex: Int, x: Any) {
    row updateObject (columnIndex, x)
  }

  def updateNull(columnLabel: String) {
    row updateNull (columnLabel)
  }

  def updateBoolean(columnLabel: String, x: Boolean) {
    row updateBoolean (columnLabel, x)
  }

  def updateByte(columnLabel: String, x: Byte) {
    row updateByte (columnLabel, x)
  }

  def updateShort(columnLabel: String, x: Short) {
    row updateShort (columnLabel, x)
  }

  def updateInt(columnLabel: String, x: Int) {
    row updateInt (columnLabel, x)
  }

  def updateLong(columnLabel: String, x: Long) {
    row updateLong (columnLabel, x)
  }

  def updateFloat(columnLabel: String, x: Float) {
    row updateFloat (columnLabel, x)
  }

  def updateDouble(columnLabel: String, x: Double) {
    row updateDouble (columnLabel, x)
  }

  def updateBigDecimal(columnLabel: String, x: BigDecimal) {
    row updateBigDecimal (columnLabel, x)
  }

  def updateString(columnLabel: String, x: String) {
    row updateString (columnLabel, x)
  }

  def updateBytes(columnLabel: String, x: Array[Byte]) {
    row updateBytes (columnLabel, x)
  }

  def updateDate(columnLabel: String, x: Date) {
    row updateDate (columnLabel, x)
  }

  def updateTime(columnLabel: String, x: Time) {
    row updateTime (columnLabel, x)
  }

  def updateTimestamp(columnLabel: String, x: Timestamp) {
    row updateTimestamp (columnLabel, x)
  }

  def updateAsciiStream(columnLabel: String, x: InputStream, length: Int) {
    row updateAsciiStream (columnLabel, x, length)
  }

  def updateBinaryStream(columnLabel: String, x: InputStream, length: Int) {
    row updateBinaryStream (columnLabel, x, length)
  }

  def updateCharacterStream(columnLabel: String, reader: Reader, length: Int) {
    row updateCharacterStream (columnLabel, reader, length)
  }

  def updateObject(columnLabel: String, x: Any, scaleOrLength: Int) {
    row updateObject (columnLabel, x, scaleOrLength)
  }

  def updateObject(columnLabel: String, x: Any) {
    row updateObject (columnLabel, x)
  }

  def insertRow() {
    row insertRow ()
  }

  def updateRow() {
    row updateRow ()
  }

  def deleteRow() {
    row deleteRow ()
  }

  def refreshRow() {
    row refreshRow ()
  }

  def cancelRowUpdates() {
    row cancelRowUpdates ()
  }

  def moveToInsertRow() {
    row moveToInsertRow ()
  }

  def moveToCurrentRow() {
    row moveToCurrentRow ()
  }

  def getStatement: Statement = row getStatement
  def getObject(columnIndex: Int, map: JMap[String, Class[_]]): AnyRef = row getObject (columnIndex, map)
  def getObject(columnLabel: String, map: JMap[String, Class[_]]): AnyRef = row getObject (columnLabel, map)
  def getObject[T](columnIndex: Int, clazz: Class[T]): T = row getObject (columnIndex, clazz)
  def getObject[T](columnLabel: String, clazz: Class[T]): T = row getObject (columnLabel, clazz)
  def getRef(columnIndex: Int) = row getRef (columnIndex)
  def getBlob(columnIndex: Int) = row getBlob (columnIndex)
  def getClob(columnIndex: Int) = row getClob (columnIndex)
  def getArray(columnIndex: Int) = row getArray (columnIndex)
  def getRef(columnLabel: String) = row getRef (columnLabel)
  def getBlob(columnLabel: String) = row getBlob (columnLabel)
  def getClob(columnLabel: String) = row getClob (columnLabel)
  def getArray(columnLabel: String) = row getArray (columnLabel)
  def getDate(columnIndex: Int, cal: Calendar): Date = row getDate (columnIndex, cal)
  def getDate(columnLabel: String, cal: Calendar): Date = row getDate (columnLabel, cal)
  def getTime(columnIndex: Int, cal: Calendar): Time = row getTime(columnIndex, cal)
  def getTime(columnLabel: String, cal: Calendar): Time = row getTime(columnLabel, cal)
  def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = row getTimestamp (columnIndex, cal)
  def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = row getTimestamp (columnLabel, cal)
  def getURL(columnIndex: Int): URL = row getURL (columnIndex)
  def getURL(columnLabel: String): URL = row getURL (columnLabel)
  def updateRef(columnIndex: Int, x: Ref) {
    row updateRef (columnIndex, x)
  }

  def updateRef(columnLabel: String, x: Ref) {
    row updateRef (columnLabel, x)
  }

  def updateBlob(columnIndex: Int, x: Blob) {
    row updateBlob (columnIndex, x)
  }

  def updateBlob(columnLabel: String, x: Blob) {
    row updateBlob (columnLabel, x)
  }

  def updateClob(columnIndex: Int, x: Clob) {
    row updateClob (columnIndex, x)
  }

  def updateClob(columnLabel: String, x: Clob) {
    row updateClob (columnLabel, x)
  }

  def updateArray(columnIndex: Int, x: java.sql.Array) {
    row updateArray (columnIndex, x)
  }

  def updateArray(columnLabel: String, x: java.sql.Array) {
    row updateArray (columnLabel, x)
  }

  def getRowId(columnIndex: Int) = row getRowId (columnIndex)
  def getRowId(columnLabel: String) = row getRowId (columnLabel)

  def updateRowId(columnIndex: Int, x: RowId) {
    row updateRowId (columnIndex, x)
  }

  def updateRowId(columnLabel: String, x: RowId) {
    row updateRowId (columnLabel, x)
  }

  def getHoldability: Int = row getHoldability
  def isClosed: Boolean = row isClosed

  def updateNString(columnIndex: Int, nString: String) {
    row updateNString (columnIndex, nString)
  }

  def updateNString(columnLabel: String, nString: String) {
    row updateNString (columnLabel, nString)
  }

  def updateNClob(columnIndex: Int, nClob: NClob) {
    row updateNClob (columnIndex, nClob)
  }

  def updateNClob(columnLabel: String, nClob: NClob) {
    row updateNClob (columnLabel, nClob)
  }

  def getNClob(columnIndex: Int) = row getNClob (columnIndex)
  def getNClob(columnLabel: String) = row getNClob (columnLabel)
  def getSQLXML(columnIndex: Int) = row getSQLXML (columnIndex)
  def getSQLXML(columnLabel: String) = row getSQLXML (columnLabel)

  def updateSQLXML(columnIndex: Int, xmlObject: SQLXML) {
    row updateSQLXML (columnIndex, xmlObject)
  }

  def updateSQLXML(columnLabel: String, xmlObject: SQLXML) {
    row updateSQLXML (columnLabel, xmlObject)
  }

  def getNString(columnIndex: Int): String = row getNString (columnIndex)
  def getNString(columnLabel: String): String = row getNString (columnLabel)
  def getNCharacterStream(columnIndex: Int): Reader = row getNCharacterStream (columnIndex)
  def getNCharacterStream(columnLabel: String): Reader = row getNCharacterStream (columnLabel)

  def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) {
    row updateNCharacterStream (columnIndex, x, length)
  }

  def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long) {
    row updateNCharacterStream (columnLabel, reader, length)
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long) {
    row updateAsciiStream (columnIndex, x, length)
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long) {
    row updateBinaryStream (columnIndex, x, length)
  }

  def updateCharacterStream(columnIndex: Int, x: Reader, length: Long) {
    row updateCharacterStream (columnIndex, x, length)
  }

  def updateAsciiStream(columnLabel: String, x: InputStream, length: Long) {
    row updateAsciiStream (columnLabel, x, length)
  }

  def updateBinaryStream(columnLabel: String, x: InputStream, length: Long) {
    row updateBinaryStream (columnLabel, x, length)
  }

  def updateCharacterStream(columnLabel: String, reader: Reader, length: Long) {
    row updateCharacterStream (columnLabel, reader, length)
  }

  def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long) {
    row updateBlob (columnIndex, inputStream, length)
  }

  def updateBlob(columnLabel: String, inputStream: InputStream, length: Long) {
    row updateBlob (columnLabel, inputStream, length)
  }

  def updateClob(columnIndex: Int, reader: Reader, length: Long) {
    row updateClob (columnIndex, reader, length)
  }

  def updateClob(columnLabel: String, reader: Reader, length: Long) {
    row updateClob (columnLabel, reader, length)
  }

  def updateNClob(columnIndex: Int, reader: Reader, length: Long) {
    row updateNClob (columnIndex, reader, length)
  }

  def updateNClob(columnLabel: String, reader: Reader, length: Long) {
    row updateNClob (columnLabel, reader, length)
  }

  def updateNCharacterStream(columnIndex: Int, x: Reader) {
    row updateNCharacterStream (columnIndex, x)
  }

  def updateNCharacterStream(columnLabel: String, reader: Reader) {
    row updateNCharacterStream (columnLabel, reader)
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream) {
    row updateAsciiStream (columnIndex, x)
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream) {
    row updateBinaryStream (columnIndex, x)
  }

  def updateCharacterStream(columnIndex: Int, x: Reader) {
    row updateCharacterStream (columnIndex, x)
  }

  def updateAsciiStream(columnLabel: String, x: InputStream) {
    row updateAsciiStream (columnLabel, x)
  }

  def updateBinaryStream(columnLabel: String, x: InputStream) {
    row updateBinaryStream (columnLabel, x)
  }

  def updateCharacterStream(columnLabel: String, reader: Reader) {
    row updateCharacterStream (columnLabel, reader)
  }

  def updateBlob(columnIndex: Int, inputStream: InputStream) {
    row updateBlob (columnIndex, inputStream)
  }

  def updateBlob(columnLabel: String, inputStream: InputStream) {
    row updateBlob (columnLabel, inputStream)
  }

  def updateClob(columnIndex: Int, reader: Reader) {
    row updateClob (columnIndex, reader)
  }

  def updateClob(columnLabel: String, reader: Reader) {
    row updateClob (columnLabel, reader)
  }

  def updateNClob(columnIndex: Int, reader: Reader) {
    row updateNClob (columnIndex, reader)
  }

  def updateNClob(columnLabel: String, reader: Reader) {
    row updateNClob (columnLabel, reader)
  }

  def unwrap[T](iface: Class[T]): T = row unwrap (iface)
  def isWrapperFor(iface: Class[_]): Boolean = row isWrapperFor (iface)
}

object RichResultSet{
  val pattern = DateTimeFormat forPattern "yyyy-MM-dd hh:mm:ss.fffffffff"

  def parseTimeStamp(time: java.sql.Timestamp) = pattern parseDateTime (time toString)
}