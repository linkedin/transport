/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spark

import java.util

import com.linkedin.transport.test.spi.SqlFunctionCallGenerator
import com.linkedin.transport.test.spi.types.TestType

import scala.collection.JavaConversions._

class SparkSqlFunctionCallGenerator extends SqlFunctionCallGenerator {

  override def getMapArgumentString(map: util.Map[AnyRef, AnyRef], mapKeyType: TestType, mapValueType: TestType): String =
    "MAP" + "(" + map.map(entry => {
      getFunctionCallArgumentString(entry._1, mapKeyType) + ", " + getFunctionCallArgumentString(entry._2, mapValueType)
    }).mkString(", ") + ")"
}
