package com.linkedin.stdudfs.spark.examples

import com.linkedin.stdudfs.spark.common.AssertSparkExpression._
import org.testng.annotations.{BeforeClass, Test}

import scala.collection.mutable.WrappedArray._

class TestMapKeySetFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "map_key_set",
      classOf[MapKeySetFunctionWrapper]
    )
  }

  @Test
  def testMapKeySet(): Unit = {
    assertFunction("map_key_set(map(1, 4, 2, 5, 3, 6))", make(Array(1, 2, 3)))
    assertFunction("map_key_set(map('1', '4', '2', '5', '3', '6'))", make(Array("1", "2", "3")))
    assertFunction("map_key_set(null)", null)
  }
}
