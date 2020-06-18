/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import java.nio.ByteBuffer
import java.util.{List => JavaList}

import com.google.common.base.Preconditions
import com.linkedin.transport.api.StdFactory
import com.linkedin.transport.api.data._
import com.linkedin.transport.api.types.StdType
import com.linkedin.transport.spark.data._
import com.linkedin.transport.spark.typesystem.SparkTypeFactory
import com.linkedin.transport.typesystem.{AbstractBoundVariables, TypeSignature}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class SparkFactory(private val _boundVariables: AbstractBoundVariables[DataType]) extends StdFactory {

  private val _sparkTypeFactory: SparkTypeFactory = new SparkTypeFactory

  override def createInteger(value: Int): StdInteger = SparkInteger(value)

  override def createLong(value: Long): StdLong = SparkLong(value)

  override def createBoolean(value: Boolean): StdBoolean = SparkBoolean(value)

  override def createString(value: String): StdString = {
    Preconditions.checkNotNull(value, "Cannot create a null StdString".asInstanceOf[Any])
    SparkString(UTF8String.fromString(value))
  }

  override def createFloat(value: Float): StdFloat = SparkFloat(value)

  override def createDouble(value: Double): StdDouble = SparkDouble(value)

  override def createBinary(value: ByteBuffer): StdBinary = {
    Preconditions.checkNotNull(value, "Cannot create a null StdBinary".asInstanceOf[Any])
    SparkBinary(value.array())
  }

  override def createArray(stdType: StdType): StdArray = createArray(stdType, 0)

  // we do not pass size to `new Array()` as the size argument of createArray is supposed to be just a hint about
  // the expected number of entries in the StdArray. `new Array(size)` will create an array with null entries
  override def createArray(stdType: StdType, size: Int): StdArray = SparkArray(
    null, stdType.underlyingType().asInstanceOf[ArrayType]
  )

  override def createMap(stdType: StdType): StdMap = SparkMap(
    //TODO: make these as separate mutable standard spark types
    null, stdType.underlyingType().asInstanceOf[MapType]
  )

  override def createStruct(fieldTypes: JavaList[StdType]): StdStruct = {
    createStruct(null, fieldTypes)
  }

  override def createStruct(fieldNames: JavaList[String], fieldTypes: JavaList[StdType]): StdStruct = {
    val structFields = new Array[StructField](fieldTypes.size())
    (0 until fieldTypes.size()).foreach({
      idx => {
        structFields(idx) = StructField(
          if (fieldNames == null) "field" + idx else fieldNames.get(idx),
          fieldTypes.get(idx).underlyingType().asInstanceOf[DataType]
        )
      }
    })
    SparkStruct(null, StructType(structFields))
  }

  override def createStruct(stdType: StdType): StdStruct = {
    //TODO: make these as separate mutable standard spark types
    val structType: StructType = stdType.underlyingType().asInstanceOf[StructType]
    SparkStruct(null, structType)
  }

  override def createStdType(typeSignature: String): StdType = SparkWrapper.createStdType(
    _sparkTypeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables))

}
