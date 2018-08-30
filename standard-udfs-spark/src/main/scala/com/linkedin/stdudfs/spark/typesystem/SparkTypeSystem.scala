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
package com.linkedin.stdudfs.spark.typesystem

import java.util

import com.linkedin.stdudfs.typesystem.AbstractTypeSystem
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class SparkTypeSystem extends AbstractTypeSystem[DataType] {
  override protected def getArrayElementType(dataType: DataType): DataType =
    dataType.asInstanceOf[ArrayType].elementType

  override protected def getMapKeyType(dataType: DataType): DataType =
    dataType.asInstanceOf[MapType].keyType

  override protected def getMapValueType(dataType: DataType): DataType =
    dataType.asInstanceOf[MapType].valueType

  override protected def getStructFieldTypes(dataType: DataType): util.List[DataType] =
    dataType.asInstanceOf[StructType].fields.map(f => f.dataType).toList.asJava

  override protected def createBooleanType(): DataType = BooleanType

  override protected def createIntegerType(): DataType = IntegerType

  override protected def createLongType(): DataType = LongType

  override protected def createStringType(): DataType = StringType

  override protected def createUnknownType(): DataType = NullType

  override protected def createArrayType(elementType: DataType): DataType =
    ArrayType(elementType)

  override protected def createMapType(keyType: DataType, valueType: DataType): DataType =
    MapType(keyType, valueType)

  override protected def createStructType(fieldNames: util.List[String], fieldTypes: util.List[DataType]): DataType = {
    if (fieldNames != null) {
      StructType(fieldNames.toArray.zip(fieldTypes.toArray).map { case (n: String, t: DataType) => StructField(n, t) })
    } else {
      val fieldNames = (0 until fieldTypes.size()).toArray.map(i => "field" + i)
      StructType(fieldNames.zip(fieldTypes.toArray).map { case (n: String, t: DataType) => StructField(n, t) })
    }
  }

  override protected def isUnknownType(dataType: DataType): Boolean = dataType.isInstanceOf[NullType]

  override protected def isBooleanType(dataType: DataType): Boolean = dataType.isInstanceOf[BooleanType]

  override protected def isIntegerType(dataType: DataType): Boolean = dataType.isInstanceOf[IntegerType]

  override protected def isLongType(dataType: DataType): Boolean = dataType.isInstanceOf[LongType]

  override protected def isStringType(dataType: DataType): Boolean = dataType.isInstanceOf[StringType]

  override protected def isArrayType(dataType: DataType): Boolean = dataType.isInstanceOf[ArrayType]

  override protected def isMapType(dataType: DataType): Boolean = dataType.isInstanceOf[MapType]

  override protected def isStructType(dataType: DataType): Boolean = dataType.isInstanceOf[StructType]
}
