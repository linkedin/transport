package com.linkedin.stdudfs.spark.typesystem

import com.linkedin.stdudfs.typesystem.{AbstractBoundVariables, AbstractTestBoundVariables, AbstractTypeSystem}
import org.apache.spark.sql.types.DataType
import org.testng.annotations.Test

@Test
class TestSparkBoundVariables extends AbstractTestBoundVariables[DataType] {

  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem

  override protected def createBoundVariables(): AbstractBoundVariables[DataType] = new SparkBoundVariables
}
