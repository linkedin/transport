/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.typesystem

import com.linkedin.transport.typesystem.{AbstractBoundVariables, AbstractTestBoundVariables, AbstractTypeSystem}
import org.apache.spark.sql.types.DataType
import org.testng.annotations.Test

@Test
class TestSparkBoundVariables extends AbstractTestBoundVariables[DataType] {

  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem

  override protected def createBoundVariables(): AbstractBoundVariables[DataType] = new SparkBoundVariables
}
