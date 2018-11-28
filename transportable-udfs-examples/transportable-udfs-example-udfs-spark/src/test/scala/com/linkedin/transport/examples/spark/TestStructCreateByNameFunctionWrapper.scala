/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.spark

import com.linkedin.transport.spark.common.AssertSparkExpression._
import org.testng.annotations.{BeforeClass, Test}

class TestStructCreateByNameFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "struct_create_by_name",
      classOf[StructCreateByNameFunctionWrapper]
    )
  }

  @Test
  def testStructCreateByNameFunction(): Unit = {
    assertFunction("cast(struct_create_by_name('a', 'x', 'b', 'y') as struct<a:string, b:string>).a", "x")
    assertFunction("cast(struct_create_by_name('a', 'x', 'b', 'y') as struct<a:string, b:string>).b", "y")
    assertFunction("struct_create_by_name(null, 'x', 'b', 'y')", null)
    assertFunction("struct_create_by_name('a', 'x', null, 'y')", null)
  }
}
