/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.typesystem

import com.linkedin.transport.api.TypeFactory
import com.linkedin.transport.typesystem.{AbstractBoundVariables, AbstractTypeFactory, AbstractTypeInference, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkTypeInference extends AbstractTypeInference[DataType] {

  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem

  override protected def createBoundVariables(): AbstractBoundVariables[DataType] = new SparkBoundVariables

  override protected def createTypeFactory(boundVariables: AbstractBoundVariables[DataType]): TypeFactory
  = new com.linkedin.transport.spark.SparkTypeFactory(boundVariables)

  override protected def getTypeFactoryFacade(): AbstractTypeFactory[DataType] = new SparkTypeFactory
}
