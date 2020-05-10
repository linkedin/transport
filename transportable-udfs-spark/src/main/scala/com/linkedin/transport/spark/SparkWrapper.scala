/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import com.linkedin.transport.api.data.PlatformData
import com.linkedin.transport.api.types.StdType
import com.linkedin.transport.spark.data._
import com.linkedin.transport.spark.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object SparkWrapper {

  def createStdData(data: Any, dataType: DataType): Object = { // scalastyle:ignore cyclomatic.complexity
    if (data == null) {
      null
    } else {
      dataType match {
        case _: IntegerType => data.asInstanceOf[Object]
        case _: LongType => data.asInstanceOf[Object]
        case _: BooleanType => data.asInstanceOf[Object]
        case _: StringType => data.asInstanceOf[UTF8String].toString
        case _: ArrayType => SparkArrayData(
          data.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData], dataType.asInstanceOf[ArrayType]
        )
        case _: MapType => SparkMapData(
          data.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData], dataType.asInstanceOf[MapType]
        )
        case _: StructType => SparkRowData(data.asInstanceOf[InternalRow], dataType.asInstanceOf[StructType])
        case _: NullType => null
        case _ => throw new UnsupportedOperationException("Unrecognized Spark Type: " + dataType.getClass)
      }
    }
  }

  def getPlatformData(transportData: Object): Object = {
    if (transportData == null) {
      null
    } else {
      transportData match {
        case _: java.lang.Integer => transportData
        case _: java.lang.Long => transportData
        case _: java.lang.Boolean => transportData
        case _: java.lang.String => UTF8String.fromString(transportData.asInstanceOf[String])
        case _ => transportData.asInstanceOf[PlatformData].getUnderlyingData
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
    case _: StructType => SparkRowType(dataType.asInstanceOf[StructType])
    case _: NullType => SparkUnknownType(dataType.asInstanceOf[NullType])
    case _ => throw new UnsupportedOperationException("Unrecognized Spark Type: " + dataType.getClass)
  }
}
