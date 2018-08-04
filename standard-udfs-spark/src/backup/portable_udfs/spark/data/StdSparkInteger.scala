package portable_udfs.spark.data

import org.apache.spark.sql.types.DataTypes
import portable_udfs.api.data.StdInteger
import portable_udfs.api.types.StdType
import portable_udfs.spark.types.StdSparkIntegerType

/**
  * Created by rratti on 8/16/17.
  */
case class StdSparkInteger(s: Integer) extends StdInteger {


  override def asStdInt(): StdSparkInteger = this

  override def getType: StdType = StdSparkIntegerType(DataTypes.IntegerType)

  override def getUnderlyingData: AnyRef = s

  override def toString: String = s.toString

  override def primitive(): Int = if(s == null) 0 else s.intValue()
}
