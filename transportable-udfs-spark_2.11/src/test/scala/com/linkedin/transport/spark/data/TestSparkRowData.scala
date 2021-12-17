/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import com.linkedin.transport.api.data.{PlatformData, RowData}
import com.linkedin.transport.spark.{SparkTypeFactory, SparkConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.testng.Assert.{assertEquals, assertNotSame, assertSame}
import org.testng.annotations.Test

class TestSparkRowData {
  val typeFactory = new SparkTypeFactory(null)
  val dataArray = Array(UTF8String.fromString("str1"), 0, 2L, false, ArrayData.toArrayData(Array.range(0, 5))) // scalastyle:ignore magic.number
  val fieldNames = Array("strField", "intField", "longField", "boolField", "arrField")
  val fieldTypes = Array(DataTypes.StringType, DataTypes.IntegerType, DataTypes.LongType, DataTypes.BooleanType,
    ArrayType(DataTypes.IntegerType))
  val structData = InternalRow.fromSeq(dataArray)
  val structType = StructType(fieldNames.indices.map(idx => StructField(fieldNames(idx), fieldTypes(idx))))

  @Test
  def testCreateSparkStruct(): Unit = {
    val row = SparkConverters.toTransportData(structData, structType).asInstanceOf[RowData]
    assertSame(row.asInstanceOf[PlatformData].getUnderlyingData, structData)
  }

  @Test
  def testSparkStructGetField(): Unit = {
    val rowData = SparkConverters.toTransportData(structData, structType).asInstanceOf[RowData]
    dataArray.indices.foreach(idx => {
      assertEquals(SparkConverters.toPlatformData(rowData.getField(idx)), dataArray(idx))
      assertEquals(SparkConverters.toPlatformData(rowData.getField(fieldNames(idx))), dataArray(idx))
    })
  }

  @Test
  def testSparkStructFields(): Unit = {
    val rowData = SparkConverters.toTransportData(structData, structType).asInstanceOf[RowData]
    assertEquals(rowData.fields().size(), structData.numFields)
    assertEquals(rowData.fields().toArray.map(f => SparkConverters.toPlatformData(f)), dataArray)
  }

  @Test
  def testSparkStructSetField(): Unit = {
    val row = SparkConverters.toTransportData(structData, structType).asInstanceOf[RowData]
    row.setField(1, 1)
    assertEquals(row.getField(1), 1)
    row.setField(fieldNames(2), 5L) // scalastyle:ignore magic.number
    assertEquals(row.getField(fieldNames(2)), 5L) // scalastyle:ignore magic.number
    // Since original InternalRow is immutable, a mutable ArrayBuffer should be created and set as the underlying object
    assertNotSame(row.asInstanceOf[PlatformData].getUnderlyingData, structData)
  }

  @Test
  def testSparkStructMutabilityReset(): Unit = {
    val row = SparkConverters.toTransportData(structData, structType).asInstanceOf[RowData]
    row.setField(1, 1)
    row.asInstanceOf[PlatformData].setUnderlyingData(structData)
    // After underlying data is explicitly set, mutable buffer should be removed
    assertSame(row.asInstanceOf[PlatformData].getUnderlyingData, structData)
  }
}
