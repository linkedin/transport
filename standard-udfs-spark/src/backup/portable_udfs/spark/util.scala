package portable_udfs.spark

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import portable_udfs.api.data._
import portable_udfs.api.types.{StdIntegerType, StdLongType, StdStringType, StdType}
import portable_udfs.spark.data._
import portable_udfs.spark.types._

object util {
  def unwrapStdType(stdType: StdType): DataType = stdType match {
    case _ : StdLongType     => DataTypes.LongType
    case _ : StdIntegerType  => DataTypes.IntegerType
    case _ : StdStringType   => DataTypes.StringType
  }

  def wrapData(data: Any, dataType: DataType): StdData = {
    if (data == null) {
      return null;
    }

    dataType match {
      case _ : IntegerType => StdSparkInteger(data.asInstanceOf[Integer])
      case _ : LongType    => StdSparkLong(data.asInstanceOf[java.lang.Long])
      case _ : StringType  => StdSparkString(data.asInstanceOf[UTF8String])
      case _ : StructType  => new StdSparkStruct(data.asInstanceOf[UnsafeRow], dataType.asInstanceOf[StructType])
      case _ : MapType     => new StdSparkMap(data.asInstanceOf[MapData], dataType.asInstanceOf[MapType])
      case _ : ArrayType   => new StdSparkArray(data.asInstanceOf[ArrayData],
        dataType.asInstanceOf[ArrayType].elementType)
      case _ : BooleanType => StdSparkInteger(0)
    }
  }

  def wrapType(dataType: DataType): StdType = dataType match {
    case _ : IntegerType => StdSparkIntegerType(dataType.asInstanceOf[IntegerType])
    case _ : LongType    => StdSparkLongType(dataType.asInstanceOf[LongType])
    case _ : StringType  => StdSparkStringType(dataType.asInstanceOf[StringType])

    case _ : ArrayType  => StdSparkArrayType(dataType.asInstanceOf[ArrayType].elementType)
    case _ : MapType    => {
      val mt = dataType.asInstanceOf[MapType]
      StdSparkMapType(mt.keyType, mt.valueType)
    }
    case _ : StructType  => StdSparkStructType(dataType.asInstanceOf[StructType])
  }
}
