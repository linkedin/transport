/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.typesystem

import com.linkedin.transport.typesystem.{AbstractTypeFactory, AbstractTypeSystem}
import org.apache.spark.sql.types._

class SparkTypeFactory extends AbstractTypeFactory[DataType] {
  override protected def getTypeSystem: AbstractTypeSystem[DataType] = new SparkTypeSystem
}
