/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdDouble}

case class SparkDouble(private var _double: java.lang.Double) extends StdDouble with PlatformData {

  override def get(): Double = _double.doubleValue()

  override def getUnderlyingData: AnyRef = _double

  override def setUnderlyingData(value: scala.Any): Unit = _double = value.asInstanceOf[java.lang.Double]
}

