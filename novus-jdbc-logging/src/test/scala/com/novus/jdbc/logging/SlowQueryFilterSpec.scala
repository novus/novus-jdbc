package com.novus.jdbc.logging

import org.specs2.mutable.Specification
import com.novus.jdbc.logging.SlowQueryFilter

class SlowQueryFilterSpec extends Specification{
  "parseTime" should{
    val filter = new SlowQueryFilter

    "handle cases where there is no time to be found" in{
      filter.parseTime("lalala timed but not there") must beNone
    }
    "handle cases where it is a query that was timed" in{
      filter.parseTime("lalala timed for 10 ms") must beSome
    }
  }
}