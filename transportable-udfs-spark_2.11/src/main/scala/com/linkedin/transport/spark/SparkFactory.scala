/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import com.linkedin.transport.api.TypeFactory
import com.linkedin.transport.api.data.{ArrayData, MapData, RowData}

import java.util.{List => JavaList}
import com.linkedin.transport.spark.data._
import com.linkedin.transport.spark.typesystem.SparkTypeFactory
import com.linkedin.transport.typesystem.{AbstractBoundVariables, TypeSignature}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

class SparkFactory(private val _boundVariables: AbstractBoundVariables[DataType]) extends TypeFactory {

  private val _sparkTypeFactory: SparkTypeFactory = new SparkTypeFactory

  override def createArray(dataType: com.linkedin.transport.api.types.DataType): ArrayData[_] = createArray(dataType, 0)

  // we do not pass size to `new Array()` as the size argument of createArray is supposed to be just a hint about
  // the expected number of entries in the ArrayData. `new Array(size)` will create an array with null entries
  override def createArray(dataType: com.linkedin.transport.api.types.DataType, size: Int): ArrayData[_] = SparkArrayData(
    null, dataType.underlyingType().asInstanceOf[ArrayType]
  )

  override def createMap(dataType: com.linkedin.transport.api.types.DataType): MapData[_, _] = SparkMapData(
    //TODO: make these as separate mutable standard spark types
    null, dataType.underlyingType().asInstanceOf[MapType]
  )

  override def createStruct(fieldTypes: JavaList[com.linkedin.transport.api.types.DataType]): RowData = {
    createStruct(null, fieldTypes)
  }

  override def createStruct(fieldNames: JavaList[String], fieldTypes: JavaList[com.linkedin.transport.api.types.DataType]): RowData = {
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

  override def createStruct(dataType: com.linkedin.transport.api.types.DataType): RowData = {
    //TODO: make these as separate mutable standard spark types
    val structType: StructType = dataType.underlyingType().asInstanceOf[StructType]
    SparkRowData(null, structType)
  }

  override def createDataType(typeSignature: String): com.linkedin.transport.api.types.DataType = SparkWrapper.createStdType(
    _sparkTypeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables))

}
