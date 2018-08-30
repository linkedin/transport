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
package com.linkedin.stdudfs.spark.types

import java.util.{List => JavaList}

import com.linkedin.stdudfs.api.types._
import com.linkedin.stdudfs.spark.SparkWrapper
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

case class SparkBooleanType(booleanType: BooleanType) extends StdBooleanType {

  override def underlyingType(): DataType = booleanType
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
