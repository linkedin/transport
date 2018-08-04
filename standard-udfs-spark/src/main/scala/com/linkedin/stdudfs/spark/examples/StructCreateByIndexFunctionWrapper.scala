package com.linkedin.stdudfs.spark.examples

import java.util

import com.google.common.collect.ImmutableList
import com.linkedin.stdudfs.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.stdudfs.examples.StructCreateByIndexFunction
import com.linkedin.stdudfs.spark.StdUdfWrapper
import org.apache.spark.sql.catalyst.expressions.Expression


case class StructCreateByIndexFunctionWrapper(expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF] = classOf[StructCreateByIndexFunction]

  override protected def getStdUdfImplementations: util.List[_ <: StdUDF] = ImmutableList.of(
    new StructCreateByIndexFunction()
  )
}
