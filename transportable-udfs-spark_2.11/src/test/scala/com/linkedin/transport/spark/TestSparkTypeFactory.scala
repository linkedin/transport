/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import com.linkedin.transport.api.data.PlatformData
import com.linkedin.transport.spark.typesystem.{SparkBoundVariables, SparkTypeFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.testng.Assert._
import org.testng.annotations.Test

import scala.collection.JavaConverters._

class TestSparkTypeFactory {

  val typeSystemTypeFactory: typesystem.SparkTypeFactory = new typesystem.SparkTypeFactory
  val typeFactory = new SparkTypeFactory(new SparkBoundVariables)

  @Test
  def testCreateArray(): Unit = {
    var array = typeFactory.createArray(typeFactory.createDataType("array(integer)"))
    assertEquals(array.size(), 0)
    assertEquals(array.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[GenericArrayData].array,
      Array.empty)
    val testArraySize = 10
    array = typeFactory.createArray(typeFactory.createDataType("array(integer)"), testArraySize)
    // size should still be 0, since size passed in createArray is just expected number of entries in the future
    assertEquals(array.size(), 0)
    assertEquals(array.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[GenericArrayData].array,
      Array.empty)
  }

  @Test
  def testCreateMap(): Unit = {
    val map = typeFactory.createMap(typeFactory.createDataType("map(varchar,bigint)"))
    assertEquals(map.size(), 0)
    assertEquals(map.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[ArrayBasedMapData].keyArray.array,
      Array.empty)
    assertEquals(map.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[ArrayBasedMapData].valueArray.array,
      Array.empty)
  }

  @Test
  def testCreateStructFromType(): Unit = {
    val fieldNames = Array("strField", "intField", "longField", "boolField", "floatField", "doubleField",
      "bytesField", "arrField")
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "real", "double", "varbinary", "array(integer)")

    val row = typeFactory.createStruct(typeFactory.createDataType(fieldNames.zip(fieldTypes).map(x => x._1 + " " + x._2).mkString("row(", ", ", ")")))
    val internalRow = row.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 8).foreach(idx => {
      assertEquals(internalRow.get(idx, typeFactory.createDataType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }

  @Test
  def testCreateStructFromFieldNamesAndTypes(): Unit = {
    val fieldNames = Array("strField", "intField", "longField", "boolField", "floatField", "doubleField",
      "bytesField", "arrField")
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "real", "double", "varbinary", "array(integer)")

    val row = typeFactory.createStruct(fieldNames.toList.asJava, fieldTypes.map(typeFactory.createDataType).toList.asJava)
    val internalRow = row.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 8).foreach(idx => {
      assertEquals(internalRow.get(idx, typeFactory.createDataType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }

  @Test
  def testCreateStructFromFieldTypes(): Unit = {
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "real", "double", "varbinary ", "array(integer)")

    val row = typeFactory.createStruct(fieldTypes.map(typeFactory.createDataType).toList.asJava)
    val internalRow = row.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 8).foreach(idx => {
      assertEquals(internalRow.get(idx, typeFactory.createDataType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }
}
