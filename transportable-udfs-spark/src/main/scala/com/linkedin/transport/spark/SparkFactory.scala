/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import java.util.{List => JavaList}

import com.linkedin.transport.api.StdFactory
import com.linkedin.transport.api.data._
import com.linkedin.transport.api.types.StdType
import com.linkedin.transport.spark.data._
import com.linkedin.transport.spark.typesystem.SparkTypeFactory
import com.linkedin.transport.typesystem.{AbstractBoundVariables, TypeSignature}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

class SparkFactory(private val _boundVariables: AbstractBoundVariables[DataType]) extends StdFactory {

  private val _sparkTypeFactory: SparkTypeFactory = new SparkTypeFactory

  override def createArray(stdType: StdType): ArrayData[_] = createArray(stdType, 0)

  // we do not pass size to `new Array()` as the size argument of createArray is supposed to be just a hint about
  // the expected number of entries in the ArrayData. `new Array(size)` will create an array with null entries
  override def createArray(stdType: StdType, size: Int): ArrayData[_] = SparkArrayData(
    null, stdType.underlyingType().asInstanceOf[ArrayType]
  )

  override def createMap(stdType: StdType): MapData[_, _] = SparkMapData(
    //TODO: make these as separate mutable standard spark types
    null, stdType.underlyingType().asInstanceOf[MapType]
  )

  override def createStruct(fieldTypes: JavaList[StdType]): RowData = {
    createStruct(null, fieldTypes)
  }

  override def createStruct(fieldNames: JavaList[String], fieldTypes: JavaList[StdType]): RowData = {
    val structFields = new Array[StructField](fieldTypes.size())
    (0 until fieldTypes.size()).foreach({
      idx => {
        structFields(idx) = StructField(
          if (fieldNames == null) "field" + idx else fieldNames.get(idx),
          fieldTypes.get(idx).underlyingType().asInstanceOf[DataType]
        )
      }
    })
    SparkRowData(null, StructType(structFields))
  }

  override def createStruct(stdType: StdType): RowData = {
    //TODO: make these as separate mutable standard spark types
    val structType: StructType = stdType.underlyingType().asInstanceOf[StructType]
    SparkRowData(null, structType)
  }

  override def createStdType(typeSignature: String): StdType = SparkWrapper.createStdType(
    _sparkTypeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables))

}
