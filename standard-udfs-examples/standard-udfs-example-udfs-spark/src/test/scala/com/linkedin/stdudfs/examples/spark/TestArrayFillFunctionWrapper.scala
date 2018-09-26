/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.spark

import com.linkedin.stdudfs.spark.common.AssertSparkExpression._
import org.apache.spark.sql.SparkSession
import org.testng.annotations.{BeforeClass, Test}

import scala.collection.mutable.WrappedArray._

class TestArrayFillFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "array_fill",
      classOf[ArrayFillFunctionWrapper]
    )
  }

  @Test
  def testArrayFill(): Unit = {
    assertFunction("array_fill(1, 5L)", make(Array(1, 1, 1, 1, 1)))
    assertFunction("array_fill('1', 5L)", make(Array("1", "1", "1", "1", "1")))
    assertFunction("array_fill(true, 5L)", make(Array(true, true, true, true, true)))
    assertFunction("array_fill(array(1), 5L)",
      make(Array(make(Array(1)), make(Array(1)), make(Array(1)), make(Array(1)), make(Array(1)))))
    assertFunction("array_fill(1, 0L)", make(Array()))
    assertFunction("array_fill(1, null)", null)
    assertFunction("array_fill(null, 1L)", null)
  }

  @Test
  def testArrayFillDataframe(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val df = 0.until(5).toDF()  // scalastyle:ignore magic.number
    df.select(callUDF("array_fill", col("value"), lit(5L))).collect()  // scalastyle:ignore magic.number
  }
}
