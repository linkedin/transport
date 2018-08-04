package com.linkedin.stdudfs.spark.typesystem

import com.linkedin.stdudfs.typesystem.{AbstractBoundVariables, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkBoundVariables extends AbstractBoundVariables[DataType] {
  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem
}
