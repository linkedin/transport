package com.linkedin.stdudfs.spark.typesystem

import com.linkedin.stdudfs.typesystem.{AbstractTypeFactory, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkTypeFactory extends AbstractTypeFactory[DataType] {
  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem
}
