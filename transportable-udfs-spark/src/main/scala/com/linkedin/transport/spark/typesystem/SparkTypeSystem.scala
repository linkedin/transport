/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.typesystem

import java.util

import com.linkedin.transport.typesystem.AbstractTypeSystem
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

  override protected def createFloatType(): DataType = FloatType

  override protected def createDoubleType(): DataType = DoubleType

  override protected def createBinaryType(): DataType = BinaryType

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

  override protected def isFloatType(dataType: DataType): Boolean = dataType.isInstanceOf[FloatType]

  override protected def isDoubleType(dataType: DataType): Boolean = dataType.isInstanceOf[DoubleType]

  override protected def isBinaryType(dataType: DataType): Boolean = dataType.isInstanceOf[BinaryType]
}
