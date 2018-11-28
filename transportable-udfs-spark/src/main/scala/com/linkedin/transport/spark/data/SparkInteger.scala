/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdInteger}

case class SparkInteger(private var _int: Integer) extends StdInteger with PlatformData {

  override def get(): Int = _int.intValue()

  override def getUnderlyingData: AnyRef = _int

  override def setUnderlyingData(value: scala.Any): Unit = _int = value.asInstanceOf[Integer]
}
