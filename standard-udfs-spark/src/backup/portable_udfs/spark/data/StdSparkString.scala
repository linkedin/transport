package portable_udfs.spark.data

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String
import portable_udfs.api.data.StdString
import portable_udfs.api.types.StdType
import portable_udfs.spark.types.StdSparkStringType

/**
  * Created by rratti on 8/16/17.
  */
case class StdSparkString(s: UTF8String) extends StdString {
  override def asStdString(): StdString = this

  override def asString(): String = s.toString

  override def getType: StdType = StdSparkStringType(DataTypes.StringType)

  override def getUnderlyingData: AnyRef = s

  override def toString: String = s.toString
}
