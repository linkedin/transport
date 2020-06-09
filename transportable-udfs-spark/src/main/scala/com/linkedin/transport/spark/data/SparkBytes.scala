/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import java.nio.ByteBuffer

import com.linkedin.transport.api.data.{PlatformData, StdBytes}

case class SparkBytes(private var _str: Array[Byte]) extends StdBytes with PlatformData {

  override def get(): ByteBuffer = ByteBuffer.wrap(_str)

  override def getUnderlyingData: AnyRef = _str

  override def setUnderlyingData(value: scala.Any): Unit = _str = value.asInstanceOf[ByteBuffer].array()
}
