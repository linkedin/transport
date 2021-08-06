/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdBoolean}

case class SparkBoolean(private var _bool: java.lang.Boolean) extends StdBoolean with PlatformData {

  override def get(): Boolean = _bool.booleanValue()

  override def getUnderlyingData: AnyRef = _bool

  override def setUnderlyingData(value: scala.Any): Unit = _bool = value.asInstanceOf[java.lang.Boolean]
}
