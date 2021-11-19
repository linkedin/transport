/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spark

import java.util

import com.linkedin.transport.api.udf.{UDF, TopLevelUDF}
import com.linkedin.transport.spark.SparkUDF
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


/**
  * A [[SparkUDF]] whose constructor takes enclosing [[UDF]] classes as parameters
  *
  * The wrapper's constructor here is parameterized so that the same wrapper can be used for all UDFs throughout the
  * test framework rather than generating UDF specific wrappers
  */
case class SparkTestSparkUDF(topLevelUdfClass: Class[_ <: TopLevelUDF], udfs: util.List[Class[_ <: UDF]],
  expressions: Seq[Expression]) extends SparkUDF(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelUDF] = topLevelUdfClass

  override protected def getUdfImplementations: util.List[_ <: UDF] = udfs.map(clazz => {
    Try(clazz.getConstructor().newInstance()) match {
      case Success(exprObject) => exprObject.asInstanceOf[UDF]
      case Failure(e) => throw new RuntimeException(e)
    }
  })
}
