/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spark

import java.util

import com.linkedin.transport.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.transport.spark.StdUdfWrapper
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


/**
  * A [[StdUdfWrapper]] whose constructor takes enclosing [[StdUDF]] classes as parameters
  *
  * The wrapper's constructor here is parameterized so that the same wrapper can be used for all UDFs throughout the
  * test framework rather than generating UDF specific wrappers
  */
case class SparkTestStdUDFWrapper(topLevelStdUdfClass: Class[_ <: TopLevelStdUDF], stdUDFs: util.List[Class[_ <: StdUDF]],
  expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF] = topLevelStdUdfClass

  override protected def getStdUdfImplementations: util.List[_ <: StdUDF] = stdUDFs.map(clazz => {
    Try(clazz.getConstructor().newInstance()) match {
      case Success(exprObject) => exprObject.asInstanceOf[StdUDF]
      case Failure(e) => throw new RuntimeException(e)
    }
  })
}
