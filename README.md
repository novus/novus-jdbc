# Novus-JDBC

A lightly opinionated and thin wrapper around the standard [Java JDBC API].

## Philosophy and Features

Adhering to KISS (Keep it simple, stupid,) Novus-JDBC does not attempt to hide from the end user the industry standard query language, SQL. There is no type-safe, composible DSL and no magical ORM layer sitting between the developer and the database. The library is not built around a specific flavor of database nor does it come prepackaged with any one database connection pool. Instead, what it does do is leverage higher order functions, lazy evaluation and other Scala goodness to create a serviceable API layer which is clean, maintainable, mockable and obvious in purpose.

#### Features

 * Lazy Evaluation of queries. `Iterators` are the primary data structure returned. Each row of the `ResultSet` is processed when it is needed and the performance cost paid for then.
 * Exposed SQL connect pools are typed to the database, reminding developers which dialect to use. 
 * Database specific JDBC code is imported implicitly via a type class.
 * Options, Eithers, and Iterables in query arguments handled transparently.
 * A more powerful ResultSet. Includes methods to aid in working with NULLABLE fields and methods for extracting [Joda-Time].
 * Works well in existing projects which may or may not have already chosen which connection pool and configuration to use.
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
executor.select("SELECT foo, bar, baz FROM myTable WHERE thing IN(?)", List(1, 2, 3)){ result =>
    Demo(result getInt "foo", result getInt "bar", result getString "baz")
}
```

 3. selectOne - returns Some if the query produces results or None, if it produces an empty set
```
executor.selectOne("SELECT foo, bar, baz FROM myTable WHERE thing IN(?)", List(1, 2, 3)){ result =>
    Demo(result getInt "foo", result getInt "bar", result getDateTime "baz")
}
```

 4. update - returns the count of total number of affected columns
```
executor update ("UPDATE foo SET bar=? WHERE baz=?", 42, 42)
```

 5. delete - returns the count of the total number of affected columns
```
executor delete ("DELETE FROM myTable WHERE thing <> 42")
```

 6. merge - returns an Iterator containing the value of the ID column of all inserted values 

#### Migrating from Querulous

For those wishing to migrate away from Querulous, it is suggested to first replace all select statements with the eager evaluation methods so as not to run into trouble with any combination of method calls like `flatMap`, `map`, or `filter` which are expecting to be chained off of `List`s.

## Alternatives

If it isn't clear by the philosophy, this library is built for those who want, need, and/or enjoy dealing directly with raw SQL. It could be used as the foundation for a more feature rich library or stand-alone as is. For those who don't want to deal with raw SQL or do not relish the thought of reimplementing a larger SQL framework, it is suggested they use one of the already available alternatives:

 1. [Querulous], a MySQL specific implementation with a similar API
 2. [Slick], a type safe, composible, SQL DSL which abstracts away the underlying DB
 3. [Squeryl], another type safe DSL which abstracts away the underlying DB yet retains the flavor of raw SQL
 4. [Circumflex], a Scala based ORM under active development and use
 5. [Hibernate], an ORM backed by a friendly, vibrant, and rather large community. For those wishing there own special little hell.



[Java JDBC API]: http://docs.oracle.com/javase/tutorial/jdbc/overview/index.html
[Joda-Time]: http://joda-time.sourceforge.net/
[slf4j]: http://www.slf4j.org/
[Logback]: http://logback.qos.ch/
[Querulous]: https://github.com/twitter/querulous
[Slick]: http://slick.typesafe.com/
[Squeryl]: http://squeryl.org/
[Circumflex]: https://github.com/inca/circumflex
[Hibernate]: http://www.hibernate.org/
