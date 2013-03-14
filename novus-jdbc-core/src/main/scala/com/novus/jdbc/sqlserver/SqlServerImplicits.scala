package com.novus.jdbc.sqlserver

import com.novus.jdbc.{RichResultSet, ResultSetWrapper, ResultSetIterator, Queryable}
import java.sql.{ResultSet, Statement, Connection}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import java.util.Calendar

trait SqlServerImplicits {
  implicit object SqlServerQueryable extends Queryable[SqlServer]

  //TODO: really could use some configs for this eventually.
  implicit object SqlServerWrapper extends ResultSetWrapper[SqlServer]{
    val pattern = DateTimeFormat forPattern "yyyy-MM-dd HH:mm:ss.SSS"
    val formatter = pattern withZone timezone()

    def timezone(id: String = "US/Eastern"): DateTimeZone = DateTimeZone forID (id)
    def timezone(cal: Calendar) = DateTimeZone forTimeZone (cal getTimeZone)

    def wrap(row: ResultSet) = new RichResultSet(row){
      override def getDateTime(column: String): DateTime ={
        val result = row getString (column)

        if(result == null || result.isEmpty) null else formatter parseDateTime (result)
      }
      override def getDateTime(column: Int): DateTime ={
        val result = row getString (column)

        if(result == null || result.isEmpty) null else formatter parseDateTime (result)
      }
      override def getDateTime(column: String, cal: Calendar): DateTime ={
        val result = row getString (column)

        if(result == null || result.isEmpty){
          null
        }
        else{
          val parser = pattern withZone timezone(cal)
          parser parseDateTime (result)
        }
      }
      override def getDateTime(column: Int, cal: Calendar): DateTime ={
        val result = row getString (column)

        if(result == null || result.isEmpty){
          null
        }
        else{
          val parser = pattern withZone timezone(cal)
          parser parseDateTime (result)
        }
      }
    }
  }
}