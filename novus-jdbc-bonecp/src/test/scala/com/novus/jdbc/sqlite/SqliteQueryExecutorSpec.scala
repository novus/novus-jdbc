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
package com.novus.jdbc.sqlite

import com.jolbox.bonecp.BoneCPConfig
import com.novus.jdbc.bonecp.DebonedQueryExecutor
import java.sql.SQLException
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.sqlite.SQLiteConfig

class SqliteQueryExecutorSpec extends Specification {


  sequential

  val log = LoggerFactory getLogger this.getClass

  val SampleDb = {
    DebonedQueryExecutor[Sqlite](
      config = {
        val c = new BoneCPConfig(new SQLiteConfig().toProperties)
        c.setJdbcUrl("jdbc:sqlite:sample.db")
        c
      },
      driver = "org.sqlite.JDBC")
  }

  "select" should {
    SampleDb.executeStatement("DROP TABLE IF EXISTS FOO")
    SampleDb.executeStatement("CREATE TABLE FOO(id IDENTITY, bar INTEGER)")
    SampleDb.executeUpdate("INSERT INTO FOO(bar) VALUES(1)") must_== 1
    SampleDb.executeUpdate("INSERT INTO FOO(bar) VALUES(2)") must_== 1
    SampleDb.executeUpdate("INSERT INTO FOO(bar) VALUES(3)") must_== 1
    "work with parameters" in {
      SampleDb.select("SELECT id FROM FOO WHERE bar = ?", 2) {
        _ getInt "id"
      }.toSeq must haveLength(1)
    }
    "work without parameters" in {
      SampleDb.select("SELECT bar FROM FOO WHERE bar = 1") {
        _ getInt "bar"
      }.toSeq must haveLength(1)
    }
  }

  "selectOne" should {

    SampleDb.executeStatement("DROP TABLE IF EXISTS BAR")
    SampleDb.executeStatement("CREATE TABLE BAR(id IDENTITY, foo INTEGER)")
    SampleDb.executeUpdate("INSERT INTO BAR(foo) VALUES(1)") must_== 1
    SampleDb.executeUpdate("INSERT INTO BAR(foo) VALUES(2)") must_== 1
    SampleDb.executeUpdate("INSERT INTO BAR(foo) VALUES(3)") must_== 1
    "work with parameters and return the one result" in {
      SampleDb.selectOne("SELECT foo FROM BAR WHERE foo = ?", 2) {
        _ getInt "foo"
      } must beSome(2)
    }
    "work with parameters and return a None when it can't be found" in {
      SampleDb.selectOne("SELECT foo FROM BAR WHERE foo = ?", 42) {
        _ getInt "foo"
      } must beNone
    }
    "work without parameters and return the one result" in {
      SampleDb.selectOne("SELECT foo FROM BAR WHERE foo = 2") {
        _ getInt "foo"
      } must beSome(2)
    }
    "work without parameters and return a None when it can't be found" in {
      SampleDb.selectOne("SELECT foo FROM BAR WHERE foo = 42") {
        _ getInt "foo"
      } must beNone
    }
  }

  "eagerlySelect" should {
    SampleDb.executeStatement("DROP TABLE IF EXISTS BAZ")
    SampleDb.executeStatement("CREATE TABLE BAZ(id IDENTITY, bar INTEGER)")
    SampleDb.executeUpdate("INSERT INTO BAZ(bar) VALUES(1)") must_== 1
    SampleDb.executeUpdate("INSERT INTO BAZ(bar) VALUES(2)") must_== 1
    SampleDb.executeUpdate("INSERT INTO BAZ(bar) VALUES(3)") must_== 1
    "work with parameters" in {
      SampleDb.eagerlySelect("SELECT bar FROM BAZ WHERE bar = ?", 2) {
        _ getInt "bar"
      } must contain(2).only
    }
    "work without parameters" in {
      SampleDb.eagerlySelect("SELECT bar FROM BAZ WHERE bar = 1") {
        _ getInt "bar"
      } must contain(1).only
    }
  }

  "insert" should {
    SampleDb.executeStatement("DROP TABLE IF EXISTS Something")
    SampleDb.executeStatement("CREATE TABLE Something(foo INTEGER, bar INTEGER, baz VARCHAR(100), PRIMARY KEY(foo, bar))")
    "work with parameters to return auto generated keys" in {
      SampleDb.insert("INSERT INTO Something(bar, baz) VALUES(?, ?)", 5, "entry").toSeq must haveLength(1)
    }
    "now work without parameters to return auto generated keys because Sqlite doesn't support this" in {
      SampleDb.insert("INSERT INTO Something(bar, baz) VALUES(3, 'yo')") must throwA[SQLException]
    }
  }

  "delete" should {

    SampleDb.executeStatement("DROP TABLE IF EXISTS GONE")
    SampleDb.executeStatement("CREATE TABLE GONE(id IDENTITY, bar INTEGER)")
    SampleDb.executeUpdate("INSERT INTO GONE(bar) VALUES(1)") must_== 1
    SampleDb.executeUpdate("INSERT INTO GONE(bar) VALUES(2)") must_== 1
    SampleDb.executeUpdate("INSERT INTO GONE(bar) VALUES(3)") must_== 1
    "work with parameters to return row deleted count" in {
      SampleDb.delete("DELETE FROM GONE WHERE bar IN(?)", List(2, 3)) must be equalTo 2
    }
    "work without parameters to return row deleted count" in {
      SampleDb.delete("DELETE FROM GONE WHERE bar = 1") must be equalTo 1
    }
  }

  "update" should {
    SampleDb.executeStatement("DROP TABLE IF EXISTS CHANGE")
    SampleDb.executeStatement("CREATE TABLE CHANGE(bar INTEGER)")
    SampleDb.executeUpdate("INSERT INTO CHANGE(bar) VALUES(1)") must_== 1
    SampleDb.executeUpdate("INSERT INTO CHANGE(bar) VALUES(2)") must_== 1
    SampleDb.executeUpdate("INSERT INTO CHANGE(bar) VALUES(3)") must_== 1
    "work with parameters to return the updated count" in {
      SampleDb.update("UPDATE CHANGE SET bar=1 WHERE bar=?", 3) must be equalTo 1
    }
    "work without parameters to return the updated count" in {
      SampleDb.update("UPDATE CHANGE SET bar=1 WHERE bar=2") must be equalTo 1
    }
  }
}
