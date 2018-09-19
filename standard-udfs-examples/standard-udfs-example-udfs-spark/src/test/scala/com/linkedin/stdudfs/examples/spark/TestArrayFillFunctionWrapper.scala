/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
