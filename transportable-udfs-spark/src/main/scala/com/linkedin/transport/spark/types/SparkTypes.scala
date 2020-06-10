/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.types

import java.util.{List => JavaList}

import com.linkedin.transport.api.types._
import com.linkedin.transport.spark.SparkWrapper
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


case class SparkIntegerType(integerType: IntegerType) extends StdIntegerType {

  override def underlyingType(): DataType = integerType
}

case class SparkLongType(longType: LongType) extends StdLongType {

  override def underlyingType(): DataType = longType
}

case class SparkStringType(stringType: StringType) extends StdStringType {

  override def underlyingType(): DataType = stringType
}

case class SparkFloatType(floatType: FloatType) extends StdFloatType {

  override def underlyingType(): DataType = floatType
}

case class SparkDoubleType(doubleType: DoubleType) extends StdDoubleType {

  override def underlyingType(): DataType = doubleType
}

case class SparkBinaryType(bytesType: BinaryType) extends StdBinaryType {

  override def underlyingType(): DataType = bytesType
}

case class SparkBooleanType(booleanType: BooleanType) extends StdBooleanType {

  override def underlyingType(): DataType = booleanType
}

case class SparkUnknownType(unknownType: NullType) extends StdUnknownType {

  override def underlyingType(): DataType = unknownType
}

case class SparkArrayType(arrayType: ArrayType) extends StdArrayType {

  override def elementType: StdType = SparkWrapper.createStdType(arrayType.elementType)

  override def underlyingType(): DataType = arrayType
}

case class SparkMapType(mapType: MapType) extends StdMapType {

  override def underlyingType(): DataType = mapType

  override def keyType(): StdType = SparkWrapper.createStdType(mapType.keyType)

  override def valueType(): StdType = SparkWrapper.createStdType(mapType.valueType)
}

case class SparkStructType(structType: StructType) extends StdStructType {

  override def underlyingType(): DataType = structType

  override def fieldTypes(): JavaList[_ <: StdType] = {
    structType.fields.map(f => SparkWrapper.createStdType(f.dataType)).toSeq.asJava
  }
}
