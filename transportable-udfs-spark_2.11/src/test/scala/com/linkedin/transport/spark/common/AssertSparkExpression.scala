/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.common

import com.linkedin.transport.spark.{StdUDFRegistration, StdUdfWrapper}
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.testng.Assert


object AssertSparkExpression {

  private val sparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("transportable-udfs")
    .getOrCreate()

  def registerStandardUdf(name: String, stdUDFWrapperClass: Class[_ <: StdUdfWrapper]): Unit = {
    StdUDFRegistration.register(name, stdUDFWrapperClass)
  }

  def assertFunction(udf: String, expected: Any) {
    import sparkSession.sql
    try {
      val result = sql("SELECT " + udf).first.get(0)
      Assert.assertEquals(result, expected)
    } catch {
      case e: SparkException => throw e.getCause()
    }
  }
}
