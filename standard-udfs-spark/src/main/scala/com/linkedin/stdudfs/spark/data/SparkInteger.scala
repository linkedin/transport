package com.linkedin.stdudfs.spark.data

import com.linkedin.stdudfs.api.data.{PlatformData, StdInteger}

case class SparkInteger(private var _int: Integer) extends StdInteger with PlatformData {

  override def get(): Int = _int.intValue()

  override def getUnderlyingData: AnyRef = _int

  override def setUnderlyingData(value: scala.Any): Unit = _int = value.asInstanceOf[Integer]
}
