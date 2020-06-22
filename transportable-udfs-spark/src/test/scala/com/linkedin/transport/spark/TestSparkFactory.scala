/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.linkedin.transport.api.data.PlatformData
import com.linkedin.transport.spark.typesystem.{SparkBoundVariables, SparkTypeFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.testng.Assert._
import org.testng.annotations.Test

import scala.collection.JavaConverters._

class TestSparkFactory {

  val typeFactory: SparkTypeFactory = new SparkTypeFactory
  val stdFactory = new SparkFactory(new SparkBoundVariables)

  @Test
  def testCreatePrimitives(): Unit = {
    assertEquals(stdFactory.createInteger(1).get(), 1)
    assertEquals(stdFactory.createLong(1L).get(), 1L)
    assertEquals(stdFactory.createBoolean(true).get(), true)
    assertEquals(stdFactory.createString("").get(), "")
    assertEquals(stdFactory.createFloat(2.0f).get(), 2.0f)
    assertEquals(stdFactory.createDouble(3.0).get(), 3.0)
    val byteArray = "foo".getBytes(Charset.forName("UTF-8"))
    assertEquals(stdFactory.createBinary(ByteBuffer.wrap(byteArray)).get().array(), byteArray)
  }

  @Test
  def testCreateArray(): Unit = {
    var stdArray = stdFactory.createArray(stdFactory.createStdType("array(integer)"))
    assertEquals(stdArray.size(), 0)
    assertEquals(stdArray.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[GenericArrayData].array,
      Array.empty)
    val testArraySize = 10
    stdArray = stdFactory.createArray(stdFactory.createStdType("array(integer)"), testArraySize)
    // size should still be 0, since size passed in createArray is just expected number of entries in the future
    assertEquals(stdArray.size(), 0)
    assertEquals(stdArray.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[GenericArrayData].array,
      Array.empty)
  }

  @Test
  def testCreateMap(): Unit = {
    val stdMap = stdFactory.createMap(stdFactory.createStdType("map(varchar,bigint)"))
    assertEquals(stdMap.size(), 0)
    assertEquals(stdMap.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[ArrayBasedMapData].keyArray.array,
      Array.empty)
    assertEquals(stdMap.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[ArrayBasedMapData].valueArray.array,
      Array.empty)
  }

  @Test
  def testCreateStructFromStdType(): Unit = {
    val fieldNames = Array("strField", "intField", "longField", "boolField", "floatField", "doubleField",
      "bytesField", "arrField")
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "real", "double", "varbinary", "array(integer)")

    val stdStruct = stdFactory.createStruct(stdFactory.createStdType(fieldNames.zip(fieldTypes).map(x => x._1 + " " + x._2).mkString("row(", ", ", ")")))
    val internalRow = stdStruct.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 8).foreach(idx => {
      assertEquals(internalRow.get(idx, stdFactory.createStdType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }

  @Test
  def testCreateStructFromFieldNamesAndTypes(): Unit = {
    val fieldNames = Array("strField", "intField", "longField", "boolField", "floatField", "doubleField",
      "bytesField", "arrField")
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "real", "double", "varbinary", "array(integer)")

    val stdStruct = stdFactory.createStruct(fieldNames.toList.asJava, fieldTypes.map(stdFactory.createStdType).toList.asJava)
    val internalRow = stdStruct.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 8).foreach(idx => {
      assertEquals(internalRow.get(idx, stdFactory.createStdType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }

  @Test
  def testCreateStructFromFieldTypes(): Unit = {
    val fieldTypes = Array("varchar", "integer", "bigint", "boolean", "real", "double", "varbinary ", "array(integer)")

    val stdStruct = stdFactory.createStruct(fieldTypes.map(stdFactory.createStdType).toList.asJava)
    val internalRow = stdStruct.asInstanceOf[PlatformData].getUnderlyingData.asInstanceOf[InternalRow]
    assertEquals(internalRow.numFields, fieldTypes.length)
    (0 until 8).foreach(idx => {
      assertEquals(internalRow.get(idx, stdFactory.createStdType(fieldTypes(idx)).underlyingType().asInstanceOf[DataType]), null)
    })
  }
}
