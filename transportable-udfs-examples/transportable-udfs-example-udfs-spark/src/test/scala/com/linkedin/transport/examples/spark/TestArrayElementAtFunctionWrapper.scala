/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.spark

import com.linkedin.transport.spark.common.AssertSparkExpression._
import org.testng.annotations.{BeforeClass, Test}

import scala.collection.mutable.WrappedArray._


class TestArrayElementAtFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "array_element_at",
      classOf[ArrayElementAtFunctionWrapper]
    )
  }

  @Test
  def testArrayElementAt(): Unit = {
    assertFunction("array_element_at(array('1', '2'), 1)", "2")
    assertFunction("array_element_at(array(1, 2), 1)", 2)
    assertFunction("array_element_at(array(true, false), 1)", false)
    assertFunction("array_element_at(array(array('1'), array('2')), 1)", make(Array("2")))
    assertFunction("array_element_at(null, 1)", null)
    assertFunction("array_element_at(array(1), null)", null)
  }
}
