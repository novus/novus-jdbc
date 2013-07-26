# Novus-JDBC

A lightly opinionated, thin wrapper around the standard [Java JDBC API].

#### Philosophy

Novus-JDBC provides several methods to wrap the main CRUD DB operations in a thin, clean, and testable API layer. Java's `Statement`, `ResultSet`, and `Connection` boilerplate is built-in behind the scenes to each member function call, freeing the end user from the worry of resource handling. The only DB specific logic that needs to be written by a developer is the SQL statement itself, everything else is abstracted away via type classes.

This library aims to follow the tenets of KISS (Keep it simple, stupid.) As a corollary and in keeping with the spirit of that philosophy, there are several things Novus-JDBC does not do. It does not attempt to hide, modify, or augment the industry standard query language, SQL. There is no type-safe or composible DSL and no magical ORM layer sitting between the developer and the database. The library is not built around any one specific flavor of database nor does it come prepackaged with a single database connection pool. Very little is assumed in an attempt to allow for maximum experimentation and flexibility of use.

#### Features

 * Lazy Evaluation of queries. `Iterator` is the primary data structure returned. Each row of a `ResultSet` is processed when iterated to and the extraction cost paid for then (i.e. no paying for 100 rows, only to take the first and throw away the remainder.)
 * Exposed SQL connect pools are typed to the database, reminding developers which dialect to use and using the compiler to enforce expected behavior.
 * Database specific JDBC code is imported implicitly via a public and overrideable type class.
 * `Option`, `Either`, and `Iterable` in query arguments handled without the end user needing to adjust the query string.
 * A more powerful `ResultSet`; includes methods to aid in working with NULL fields, sane defaults for numeric types and methods for extracting [Joda-Time].
 * Works well in existing projects which may have already chosen which connection pool and configuration to use.
 * Logging of all queries through an [slf4j] interface. Includes an optional implementation of a configurable [Logback] slow query filter.
 * Built-in connection pool resource and exception handling.*

## API Overview

There are six main API calls which mimic the standard CRUD nomenclature:

 1. insert - returns an Iterator containing the values of the ID column of all inserted values
```
val ids = executor insert ("INSERT INTO myTable VALUES(?, ?)", 42, "the answer")
```

 2. select - returns an Iterator which maps over the ResultSet
```
val values = executor.select("SELECT foo, bar, baz FROM myTable WHERE thing IN(?)", List(1, 2, 3)){ result =>
    Demo(result getInt "foo", result getInt "bar", result getString "baz")
}
```

 3. selectOne - returns Some if the query produces results or None, if it produces an empty set
```
val item = executor.selectOne("SELECT foo, bar, baz FROM myTable WHERE thing IN(?)", List(1, 2, 3)){ result =>
    Demo(result getInt "foo", result getInt "bar", result getDateTime "baz")
}
```

 4. update - returns the count of total number of affected columns
```
val count = executor update ("UPDATE foo SET bar=? WHERE baz=?", 42, 42)
```

 5. delete - returns the count of the total number of affected columns
```
val count = executor delete ("DELETE FROM myTable WHERE thing <> 42")
```

 6. merge - returns an Iterator containing the value of the ID column of all inserted values 

There are also eagerly evaluated select methods and support for insertion into tables with multi-column, auto-generated, compound primary keys.

#### Migrating from Querulous

For those wishing to migrate away from Querulous, it is suggested to first replace all select statements with the eager evaluation methods so as not to run into trouble with any combination of method calls like `flatMap`, `map`, or `filter` which are expecting to be chained off of `List` return types.

#### Future Plans

 1. Support for transactions.
 2. Support for stored procedures.
 3. More databases
 4. Default insertion logic of Joda's `DateTime` objects.

## Alternatives

If it isn't clear by the philosophy, this library is built for those who want, need, and/or enjoy dealing directly with raw SQL. It could be used as the foundation for a more feature rich library or stand-alone as is. For those who don't want to deal with raw SQL, are working with MySQL/MariaDd or do not relish the thought of reimplementing a larger SQL framework, it is suggested they use one of the already available alternatives:

 1. [Querulous], a MySQL specific implementation with a similar API
 2. [Slick], a type safe, composible, SQL DSL which abstracts away the underlying DB
 3. [Squeryl], another type safe DSL which abstracts away the underlying DB yet retains the flavor of raw SQL
 4. [Circumflex], a Scala based ORM under active development and use
 5. [Hibernate], an ORM backed by a friendly, vibrant, and rather large community. For those wishing there own special little hell.
 6. [SORM], a functional and boilerplate free Scala ORM framework


[Java JDBC API]: http://docs.oracle.com/javase/tutorial/jdbc/overview/index.html
[Joda-Time]: http://joda-time.sourceforge.net/
[slf4j]: http://www.slf4j.org/
[Logback]: http://logback.qos.ch/
[Querulous]: https://github.com/twitter/querulous
[Slick]: http://slick.typesafe.com/
[Squeryl]: http://squeryl.org/
[Circumflex]: https://github.com/inca/circumflex
[Hibernate]: http://www.hibernate.org/
[SORM]: http://sorm-framework.org/
