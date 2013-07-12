package com.novus.jdbc.sqlserver

import com.novus.jdbc.{RichResultSet, Queryable}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import java.util.Calendar
import java.sql.ResultSet

object `package` extends SqlServerImplicits

sealed trait SqlServer

trait SqlServerImplicits {
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
  }
}