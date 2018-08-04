package com.linkedin.stdudfs.spark

import com.linkedin.stdudfs.api.data._
import com.linkedin.stdudfs.api.types._
import com.linkedin.stdudfs.spark.data._
import com.linkedin.stdudfs.spark.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object SparkWrapper {

  def createStdData(data: Any, dataType: DataType): StdData = { // scalastyle:ignore cyclomatic.complexity
    if (data == null) {
      null
    } else {
      dataType match {
        case _: IntegerType => SparkInteger(data.asInstanceOf[Integer])
        case _: LongType => SparkLong(data.asInstanceOf[java.lang.Long])
        case _: BooleanType => SparkBoolean(data.asInstanceOf[java.lang.Boolean])
        case _: StringType => SparkString(data.asInstanceOf[UTF8String])
        case _: ArrayType => SparkArray(data.asInstanceOf[ArrayData], dataType.asInstanceOf[ArrayType])
        case _: MapType => SparkMap(data.asInstanceOf[MapData], dataType.asInstanceOf[MapType])
        case _: StructType => SparkStruct(data.asInstanceOf[InternalRow], dataType.asInstanceOf[StructType])
        case _: NullType => null
        case _ => throw new UnsupportedOperationException("Unrecognized Spark Type: " + dataType.getClass)
      }
    }
  }

  def createStdType(dataType: DataType): StdType = dataType match {
    case _: IntegerType => SparkIntegerType(dataType.asInstanceOf[IntegerType])
    case _: LongType => SparkLongType(dataType.asInstanceOf[LongType])
    case _: BooleanType => SparkBooleanType(dataType.asInstanceOf[BooleanType])
    case _: StringType => SparkStringType(dataType.asInstanceOf[StringType])
    case _: ArrayType => SparkArrayType(dataType.asInstanceOf[ArrayType])
    case _: MapType => SparkMapType(dataType.asInstanceOf[MapType])
    case _: StructType => SparkStructType(dataType.asInstanceOf[StructType])
    case _: NullType => null
    case _ => throw new UnsupportedOperationException("Unrecognized Spark Type: " + dataType.getClass)
  }
}
