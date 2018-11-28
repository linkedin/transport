/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.spark

import com.linkedin.transport.spark.common.AssertSparkExpression._
import org.apache.spark.sql.Row
import org.testng.annotations.{BeforeClass, Test}

import scala.collection.mutable.WrappedArray._

class TestStructCreateByIndexFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "struct_create_by_index",
      classOf[StructCreateByIndexFunctionWrapper]
    )
  }

  @Test
  def testStructCreateByIndexFunction(): Unit = {
    assertFunction("struct_create_by_index('x', 'y')", Row("x", "y"))
    assertFunction("struct_create_by_index(1, array(1))", Row(1, make(Array(1))))
    assertFunction("struct_create_by_index(null, null)", null)
  }
}
