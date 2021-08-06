/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.sql


import com.linkedin.transport.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.transport.spark.StdUdfWrapper
import com.linkedin.transport.test.spark.SparkTestStdUDFWrapper
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.util.{Failure, Success, Try}

/**
  * Helper methods for registration of [[SparkTestStdUDFWrapper]] with Spark's
  * [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
  */
object StdUDFTestUtils {

  private def functionBuilder[T <: StdUdfWrapper](topLevelStdUdfClass: Class[_ <: TopLevelStdUDF],
    stdUDFs: java.util.List[Class[_ <: StdUDF]]): FunctionBuilder = {
    children: Seq[Expression] => {
      Try(classOf[SparkTestStdUDFWrapper].getDeclaredConstructor(
        classOf[Class[_ <: TopLevelStdUDF]],
        classOf[java.util.List[_ <: StdUDF]],
        classOf[Seq[Expression]]
      ).newInstance(topLevelStdUdfClass, stdUDFs, children)) match {
        case Success(exprObject) => exprObject.asInstanceOf[Expression]
        case Failure(e) => throw new IllegalStateException(e)
      }
    }
  }

  def register[T <: StdUdfWrapper](name: String, topLevelStdUdfClass: Class[_ <: TopLevelStdUDF],
    stdUDFs: java.util.List[Class[_ <: StdUDF]], sparkSession: SparkSession): Unit = {
    val registry = sparkSession.sessionState.functionRegistry
    registry.registerFunction(FunctionIdentifier(name), functionBuilder(topLevelStdUdfClass, stdUDFs))
  }
}
