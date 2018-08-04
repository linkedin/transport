package com.linkedin.stdudfs.spark.data

import com.linkedin.stdudfs.api.data.{PlatformData, StdLong}

case class SparkLong(private var _long: java.lang.Long) extends StdLong with PlatformData {

  override def get(): Long = _long.longValue()

  override def getUnderlyingData: AnyRef = _long

  override def setUnderlyingData(value: scala.Any): Unit = _long = value.asInstanceOf[java.lang.Long]

}
