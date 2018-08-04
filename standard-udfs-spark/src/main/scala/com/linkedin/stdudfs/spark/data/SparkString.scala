package com.linkedin.stdudfs.spark.data

import com.linkedin.stdudfs.api.data.{PlatformData, StdString}
import org.apache.spark.unsafe.types.UTF8String

case class SparkString(private var _str: UTF8String) extends StdString with PlatformData {

  override def get(): String = _str.toString

  override def getUnderlyingData: AnyRef = _str

  override def setUnderlyingData(value: scala.Any): Unit = _str = value.asInstanceOf[UTF8String]
}
