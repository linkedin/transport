/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.spark.typesystem

import com.linkedin.stdudfs.api.StdFactory
import com.linkedin.stdudfs.spark.SparkFactory
import com.linkedin.stdudfs.typesystem.{AbstractBoundVariables, AbstractTypeFactory, AbstractTypeInference, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkTypeInference extends AbstractTypeInference[DataType] {

  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem

  override protected def createBoundVariables(): AbstractBoundVariables[DataType] = new SparkBoundVariables

  override protected def createStdFactory(boundVariables: AbstractBoundVariables[DataType]): StdFactory = new SparkFactory(boundVariables)

  override protected def getTypeFactory(): AbstractTypeFactory[DataType] = new SparkTypeFactory
}
