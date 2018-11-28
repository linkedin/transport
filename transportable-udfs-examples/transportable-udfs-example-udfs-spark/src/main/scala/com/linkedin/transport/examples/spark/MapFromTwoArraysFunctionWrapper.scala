/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.spark

import java.util

import com.google.common.collect.ImmutableList
import com.linkedin.transport.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.transport.examples.MapFromTwoArraysFunction
import com.linkedin.transport.spark.StdUdfWrapper
import org.apache.spark.sql.catalyst.expressions.Expression


case class MapFromTwoArraysFunctionWrapper(expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF] = classOf[MapFromTwoArraysFunction]

  override protected def getStdUdfImplementations: util.List[_ <: StdUDF] = ImmutableList.of(
    new MapFromTwoArraysFunction()
  )
}
