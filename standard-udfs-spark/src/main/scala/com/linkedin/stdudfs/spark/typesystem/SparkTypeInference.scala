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
