/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.typesystem

import com.linkedin.transport.api.StdFactory
import com.linkedin.transport.spark.SparkFactory
import com.linkedin.transport.typesystem.{AbstractBoundVariables, AbstractTypeFactory, AbstractTypeInference, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkTypeInference extends AbstractTypeInference[DataType] {

  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem

  override protected def createBoundVariables(): AbstractBoundVariables[DataType] = new SparkBoundVariables

  override protected def createStdFactory(boundVariables: AbstractBoundVariables[DataType]): StdFactory = new SparkFactory(boundVariables)

  override protected def getTypeFactory(): AbstractTypeFactory[DataType] = new SparkTypeFactory
}
