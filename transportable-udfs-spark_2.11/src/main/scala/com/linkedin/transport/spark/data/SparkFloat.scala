/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdFloat}

case class SparkFloat(private var _float: java.lang.Float) extends StdFloat with PlatformData {

  override def get(): Float = _float.floatValue()

  override def getUnderlyingData: AnyRef = _float

  override def setUnderlyingData(value: scala.Any): Unit = _float = value.asInstanceOf[java.lang.Float]
}
