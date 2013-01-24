package com.novus.jdbc

import java.sql.{ResultSet, Statement, Connection}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import java.util.Calendar

//TODO: think about splitting up the DB specific things into their own section.
object `package` extends SqlServerImplicits

trait SqlServer

trait SqlServerImplicits {
  implicit object SqlServerQueryable extends Queryable[SqlServer] {

    override def insert(con: Connection, q: String, params: Any*): Iterator[Int] = {
      val prepared = con.prepareStatement(q, Statement.RETURN_GENERATED_KEYS)
      val stmt = statement(prepared, params: _*)
      stmt.executeUpdate()
      val keys = stmt.getGeneratedKeys

      /* SQL Server is "1" array based indexing. Guess starting from "0" like the rest of the world wasn't for them. */
      new ResultSetIterator[ResultSet,Int](keys, _ getInt (1))
    }
  }

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
