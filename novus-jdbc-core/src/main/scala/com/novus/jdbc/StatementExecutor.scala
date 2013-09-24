package com.novus.jdbc

/** A common interface from which to work independent of context of statement calls, i.e. within transactions or not. By
 *  popular demand.
 *
 * @tparam DBType The database type
 */
trait StatementExecutor[DBType]{

  /** Returns an iterator containing update counts. */
  def executeBatch[I <: Seq[Any]](batchSize: Int = 1000)(q: String, params: Iterator[I])(implicit query: Queryable[DBType]): Iterator[Int]

  /**
   * Execute a query and transform only the head of the `RichResultSet`. If this query would produce multiple results,
   * they are lost.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  def selectOne[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T]

  /**
   * Execute a query and transform only the head of the [[com.novus.jdbc.RichResultSet]]. If this query would produce
   * multiple results, they are lost.
   *
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  def selectOne[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T]

  /**
   * Execute a query and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress through the
   * underlying `RichResultSet` and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  def select[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Execute a query and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress through the
   * underlying `RichResultSet` and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The returned type from the query
   */
  def select[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Eagerly evaluates the argument function against the returned [[com.novus.jdbc.RichResultSet]].
   *
   * @see #select
   */
  def eagerlySelect[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): List[T]

  /**
   * Eagerly evaluates the argument function against the returned `RichResultSet`.
   *
   * @see #select
   */
  def eagerlySelect[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): List[T]

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  def insert(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int]

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  def insert[T](columns: Array[Int], q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The return type of the query
   */
  def insert[T](columns: Array[String], q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   */
  def insert(q: String)(implicit query: Queryable[DBType]): CloseableIterator[Int]

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The return type of the query
   */
  def insert[T](columns: Array[Int], q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  def insert[T](columns: Array[String], q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  def update(q: String, params: Any*)(implicit query: Queryable[DBType]): Int

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   *
   * @param q The query statement
   */
  def update(q: String)(implicit query: Queryable[DBType]): Int

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  def delete(q: String, params: Any*)(implicit query: Queryable[DBType]): Int

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   */
  def delete(q: String)(implicit query: Queryable[DBType]): Int

  /**
   * Returns an iterator containing the ID column which was inserted as a result of the merge statement. If this merge
   * statement does not cause an insertion into a table generating new IDs, the iterator returns empty. It is suggested
   * that update be used in the case where row counts affected is preferable to IDs.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  def merge(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int]

  /**
   * Returns an iterator containing the ID column which was inserted as a result of the merge statement. If this merge
   * statement does not cause an insertion into a table generating new IDs, the iterator returns empty. It is suggested
   * that update be used in the case where row counts affected is preferable to IDs.
   *
   * @param q The query statement
   */
  def merge(q: String)(implicit query: Queryable[DBType]): CloseableIterator[Int]

  /**
   * Execute a stored procedure and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress
   * through the underlying [[com.novus.jdbc.RichResultSet]] and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  def proc[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Execute a stored procedure and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress
   * through the underlying [[com.novus.jdbc.RichResultSet]] and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  def proc[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T]

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[Int], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[String], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[Int], q: String, params: Any*)(f: StatementResult => T)(implicit query: Queryable[DBType]): T

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[String], q: String, params: Any*)(f: StatementResult => T)(implicit query: Queryable[DBType]): T
}