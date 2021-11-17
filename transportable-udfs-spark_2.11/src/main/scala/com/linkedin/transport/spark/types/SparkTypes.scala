/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.types

import com.linkedin.transport.api.types

import java.util.{List => JavaList}
import com.linkedin.transport.spark.SparkWrapper
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


case class SparkIntegerType(integerType: IntegerType) extends types.IntegerType {

  override def underlyingType(): DataType = integerType
}

case class SparkLongType(longType: LongType) extends types.LongType {

  override def underlyingType(): DataType = longType
}

case class SparkStringType(stringType: StringType) extends com.linkedin.transport.api.types.StringType {

  override def underlyingType(): DataType = stringType
}

case class SparkFloatType(floatType: FloatType) extends types.FloatType {

  override def underlyingType(): DataType = floatType
}

case class SparkDoubleType(doubleType: DoubleType) extends types.DoubleType {

  override def underlyingType(): DataType = doubleType
}

case class SparkBinaryType(bytesType: BinaryType) extends types.BinaryType {

  override def underlyingType(): DataType = bytesType
}

case class SparkBooleanType(booleanType: BooleanType) extends types.BooleanType {

  override def underlyingType(): DataType = booleanType
}

case class SparkUnknownType(unknownType: NullType) extends com.linkedin.transport.api.types.UnknownType {

  override def underlyingType(): DataType = unknownType
}

case class SparkArrayType(arrayType: ArrayType) extends types.ArrayType {

  override def elementType: com.linkedin.transport.api.types.DataType = SparkWrapper.createStdType(arrayType.elementType)

  override def underlyingType(): DataType = arrayType
}

case class SparkMapType(mapType: MapType) extends types.MapType {

  override def underlyingType(): DataType = mapType

  override def keyType(): com.linkedin.transport.api.types.DataType = SparkWrapper.createStdType(mapType.keyType)

  override def valueType(): com.linkedin.transport.api.types.DataType = SparkWrapper.createStdType(mapType.valueType)
}

case class SparkRowType(structType: StructType) extends com.linkedin.transport.api.types.RowType {

  override def underlyingType(): DataType = structType

  override def fieldTypes(): JavaList[_ <: com.linkedin.transport.api.types.DataType] = {
    structType.fields.map(f => SparkWrapper.createStdType(f.dataType)).toSeq.asJava
  }
}
