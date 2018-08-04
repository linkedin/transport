package com.linkedin.stdudfs.spark.examples

import com.linkedin.stdudfs.spark.common.AssertSparkExpression._
import org.testng.annotations.{BeforeClass, Test}

import scala.collection.mutable.WrappedArray._

class TestMapValuesFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "std_map_values",
      classOf[MapValuesFunctionWrapper]
    )
  }

  @Test
  def testMapValues(): Unit = {
    assertFunction("std_map_values(map(1, 4, 2, 5, 3, 6))", make(Array(4, 5, 6))) // scalastyle:ignore magic.number
    assertFunction("std_map_values(map('1', '4', '2', '5', '3', '6'))", make(Array("4", "5", "6")))
    assertFunction("std_map_values(null)", null)
  }
}
