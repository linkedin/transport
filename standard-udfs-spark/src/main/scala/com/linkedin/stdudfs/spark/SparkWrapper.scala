/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
