package com.linkedin.stdudfs.spark.data

import com.linkedin.stdudfs.api.data.{PlatformData, StdBoolean}

case class SparkBoolean(private var _bool: java.lang.Boolean) extends StdBoolean with PlatformData {

  override def get(): Boolean = _bool.booleanValue()

  override def getUnderlyingData: AnyRef = _bool

  override def setUnderlyingData(value: scala.Any): Unit = _bool = value.asInstanceOf[java.lang.Boolean]
}
