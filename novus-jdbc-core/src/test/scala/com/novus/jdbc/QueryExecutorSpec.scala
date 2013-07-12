package com.novus.jdbc

import org.specs2.mutable.Specification
import java.sql.DriverManager

class QueryExecutorSpec extends Specification{
  Class.forName("org.hsqldb.jdbcDriver")

  def con() = DriverManager getConnection ("jdbc:hsqldb:mem:aname", "SA", "")

  object TestExecutor extends QueryExecutor[HSQL]{
    protected def connection() = con()

    def shutdown(){}
  }

  Class.forName("org.h2.Driver")

  "select" should {
    val connect = con()
    val stmt = connect createStatement ()
    stmt execute ("CREATE TABLE FOO(id IDENTITY, bar INTEGER)")
    stmt executeUpdate ("INSERT INTO FOO(bar) VALUES(1)")
    stmt executeUpdate ("INSERT INTO FOO(bar) VALUES(2)")
    stmt executeUpdate ("INSERT INTO FOO(bar) VALUES(3)")
    stmt close ()

    "work with parameters" in {
      val out = TestExecutor.select("SELECT id FROM FOO WHERE bar = ?", 2){ _ getInt "id" }

      out must haveLength(1)
    }
    "work without parameters" in {
      val out = TestExecutor.select("SELECT bar FROM FOO WHERE id = 1"){ _ getInt "bar" }

      out must haveLength(1)
    }
  }

  "selectOne" should {
    val connect = con()
    val stmt = connect createStatement ()
    stmt execute ("CREATE TABLE BAR(id IDENTITY, foo INTEGER)")
    stmt executeUpdate ("INSERT INTO BAR(foo) VALUES(1)")
    stmt executeUpdate ("INSERT INTO BAR(foo) VALUES(2)")
    stmt executeUpdate ("INSERT INTO BAR(foo) VALUES(3)")
    stmt close ()

    "work with parameters and return the one result" in {
      val out = TestExecutor.selectOne("SELECT foo FROM BAR WHERE foo = ?", 2){ _ getInt "foo" }

      out must beSome(2)
    }
    "work with parameters and return a None when it can't be found" in {
      val out = TestExecutor.selectOne("SELECT foo FROM BAR WHERE foo = ?", 42){ _ getInt "foo" }

      out must beNone
    }
    "work without parameters and return the one result" in {
      val out = TestExecutor.selectOne("SELECT foo FROM BAR WHERE foo = 2"){ _ getInt "foo" }

      out must beSome(2)
    }
    "work without parameters and return a None when it can't be found" in {
      val out = TestExecutor.selectOne("SELECT foo FROM BAR WHERE foo = 42"){ _ getInt "foo" }

      out must beNone
    }
  }

  "eagerlySelect" should {
    val connect = con()
    val stmt = connect createStatement ()
    stmt execute ("CREATE TABLE BAZ(id IDENTITY, bar INTEGER)")
    stmt executeUpdate ("INSERT INTO BAZ(bar) VALUES(1)")
    stmt executeUpdate ("INSERT INTO BAZ(bar) VALUES(2)")
    stmt executeUpdate ("INSERT INTO BAZ(bar) VALUES(3)")
    stmt close ()

    "work with parameters" in {
      val out = TestExecutor.eagerlySelect("SELECT bar FROM BAZ WHERE bar = ?", 2){ _ getInt "bar" }

      (out must haveLength(1)) and
        (out must contain(2))
    }
    "work without parameters" in {
      val out = TestExecutor.eagerlySelect("SELECT bar FROM BAZ WHERE bar = 1"){ _ getInt "bar" }

      (out must haveLength(1)) and
        (out must contain(1))
    }
  }

  "insert" should {
    val connect = con()
    val stmt = connect createStatement ()
    stmt execute "CREATE TABLE Something(foo INTEGER GENERATED ALWAYS AS IDENTITY, bar INTEGER, baz VARCHAR(100), PRIMARY KEY(foo, bar))"
    stmt close ()

    "work with paramters to return auto generated keys" in{
      val out = TestExecutor.insert("INSERT INTO Something(bar, baz) VALUES(?, ?)", 5, "entry")

      out must haveLength(1)
    }
    "work without parameters to return auto generated keys" in {
      val out = TestExecutor.insert("INSERT INTO Something(bar, baz) VALUES(3, 'yo')")

      out must haveLength(1)
    }
  }

  "delete" should {
    val connect = con()
    val stmt = connect createStatement ()
    stmt execute ("CREATE TABLE GONE(bar INTEGER)")
    stmt executeUpdate ("INSERT INTO GONE(bar) VALUES(1)")
    stmt executeUpdate ("INSERT INTO GONE(bar) VALUES(2)")
    stmt executeUpdate ("INSERT INTO GONE(bar) VALUES(3)")
    stmt close ()

    "work with parameters to return row deleted count" in{
      TestExecutor.delete("DELETE FROM GONE WHERE bar IN(?)", List(2,3)) must be equalTo(2)
    }
    "work without parameters to return row deleted count" in{
      TestExecutor.delete("DELETE FROM GONE WHERE bar = 1") must be equalTo(1)
    }
  }

  "update" should {
    val connect = con()
    val stmt = connect createStatement ()
    stmt execute ("CREATE TABLE CHANGE(bar INTEGER)")
    stmt executeUpdate ("INSERT INTO CHANGE(bar) VALUES(1)")
    stmt executeUpdate ("INSERT INTO CHANGE(bar) VALUES(2)")
    stmt executeUpdate ("INSERT INTO CHANGE(bar) VALUES(3)")
    stmt close ()

    "work with parameters to return the updated count" in{
      TestExecutor.update("UPDATE CHANGE SET bar=1 WHERE bar=?", 3) must be equalTo(1)
    }
    "work without parameters to return the updated count" in{
      TestExecutor.update("UPDATE CHANGE SET bar=1 WHERE bar=2") must be equalTo(1)
    }
  }
}