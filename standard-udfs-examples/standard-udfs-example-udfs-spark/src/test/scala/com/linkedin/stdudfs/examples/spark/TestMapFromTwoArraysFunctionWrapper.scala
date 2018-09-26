/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.spark

import com.linkedin.stdudfs.spark.common.AssertSparkExpression._
import org.testng.annotations.{BeforeClass, Test}

import scala.collection.mutable.WrappedArray._

class TestMapFromTwoArraysFunctionWrapper {

  @BeforeClass
  def registerFunction(): Unit = {
    registerStandardUdf(
      "map_from_two_arrays",
      classOf[MapFromTwoArraysFunctionWrapper]
    )
  }

  @Test
  def testMapFromTwoArrays(): Unit = {
    assertFunction("map_from_two_arrays(array(1,2), array('a', 'b'))", Map((1, "a"), (2, "b")))

    assertFunction("map_from_two_arrays(array(array(1), array(2)), array(array('a'), array('b')))",
      Map((make(Array(1)), make(Array("a"))), (make(Array(2)), make(Array("b")))))

    assertFunction("map_from_two_arrays(null, array(array('a'), array('b')))", null)
    assertFunction("map_from_two_arrays(array(array(1), array(2)), null)", null)
  }
}
