/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.spark

import java.util

import com.google.common.collect.ImmutableList
import com.linkedin.stdudfs.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.stdudfs.examples.StructCreateByNameFunction
import com.linkedin.stdudfs.spark.StdUdfWrapper
import org.apache.spark.sql.catalyst.expressions.Expression


case class StructCreateByNameFunctionWrapper(expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF] = classOf[StructCreateByNameFunction]

  override protected def getStdUdfImplementations: util.List[_ <: StdUDF] = ImmutableList.of(
    new StructCreateByNameFunction()
  )
}
