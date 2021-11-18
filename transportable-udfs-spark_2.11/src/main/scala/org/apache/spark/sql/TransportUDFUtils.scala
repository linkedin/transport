/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.sql

import com.linkedin.transport.spark.SparkUDF
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.util.{Failure, Success, Try}

object TransportUDFUtils {

  private def functionBuilder[T <: SparkUDF](sparkUDFClass: Class[T]): FunctionBuilder = {
    (children: Seq[Expression]) => {
      Try(sparkUDFClass.getDeclaredConstructor(classOf[Seq[Expression]]).newInstance(children)) match {
        case Success(exprObject) => exprObject.asInstanceOf[Expression]
        case Failure(e) => throw new IllegalStateException(e)
      }
    }
  }

  def register[T <: SparkUDF](name: String, sparkUDFClass: Class[T], sparkSession: SparkSession): Unit = {
    val registry = sparkSession.sessionState.functionRegistry
    registry.registerFunction(FunctionIdentifier(name), functionBuilder(sparkUDFClass))
  }
}
