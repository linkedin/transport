package portable_udfs.spark.data

import org.apache.spark.sql.types.DataTypes
import portable_udfs.api.data.StdLong
import portable_udfs.api.types.StdType
import portable_udfs.spark.util

/**
  * Created by rratti on 8/16/17.
  */
case class StdSparkLong(s: java.lang.Long) extends StdLong {
  override def asStdLong(): StdSparkLong = this

  override def getType: StdType = util.wrapType(DataTypes.LongType)

  override def getUnderlyingData: AnyRef = s

  override def toString: String = s.toString

  override def primitive(): Long = if (s == null) 0 else s.longValue()
}
