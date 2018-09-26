/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.spark.typesystem

import com.linkedin.stdudfs.typesystem.{AbstractTypeFactory, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkTypeFactory extends AbstractTypeFactory[DataType] {
  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem
}
