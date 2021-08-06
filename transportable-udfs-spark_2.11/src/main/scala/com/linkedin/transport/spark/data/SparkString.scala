/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, StdString}
import org.apache.spark.unsafe.types.UTF8String

case class SparkString(private var _str: UTF8String) extends StdString with PlatformData {

  override def get(): String = _str.toString

  override def getUnderlyingData: AnyRef = _str

  override def setUnderlyingData(value: scala.Any): Unit = _str = value.asInstanceOf[UTF8String]
}
