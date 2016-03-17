package com.novus.jdbc.hikari

import com.novus.jdbc.{QueryExecutor, Queryable}
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.pool.HikariPool

class HikariQueryExecutor[DBType: Queryable](pool: HikariPool) extends QueryExecutor[DBType] {
  protected def connection() = pool.getConnection

  def shutdown() = pool.shutdown()
}

object HikariQueryExecutor {

  def apply[DBType: Queryable](config: HikariConfig, driver: String): HikariQueryExecutor[DBType] = {
    Class.forName(driver)

    new HikariQueryExecutor[DBType](new HikariPool(config))
  }
}
