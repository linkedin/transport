/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdLong}

case class SparkLong(private var _long: java.lang.Long) extends StdLong with PlatformData {

  override def get(): Long = _long.longValue()

  override def getUnderlyingData: AnyRef = _long

  override def setUnderlyingData(value: scala.Any): Unit = _long = value.asInstanceOf[java.lang.Long]

}
