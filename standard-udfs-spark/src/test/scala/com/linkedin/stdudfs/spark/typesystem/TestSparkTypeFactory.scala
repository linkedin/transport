package com.linkedin.stdudfs.spark.typesystem

import com.linkedin.stdudfs.typesystem.{AbstractBoundVariables, AbstractTestTypeFactory, AbstractTypeFactory, AbstractTypeSystem}
import org.apache.spark.sql.types.DataType
import org.testng.annotations.Test

@Test
class TestSparkTypeFactory extends AbstractTestTypeFactory[DataType] {

  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem

  override protected def createBoundVariables(): AbstractBoundVariables[DataType] = new SparkBoundVariables

  override protected def getTypeFactory: AbstractTypeFactory[DataType] = new SparkTypeFactory
}
