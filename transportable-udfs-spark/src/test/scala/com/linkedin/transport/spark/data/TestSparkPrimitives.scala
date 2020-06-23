/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark.data

import java.lang
import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.linkedin.transport.api.data._
import com.linkedin.transport.spark.{SparkFactory, SparkWrapper}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String
import org.testng.Assert.{assertEquals, assertSame}
import org.testng.annotations.Test


class TestSparkPrimitives {

  val stdFactory = new SparkFactory(null)

  @Test
  def testCreateSparkInteger(): Unit = {
    val intData = 123
    val stdInteger = SparkWrapper.createStdData(intData, DataTypes.IntegerType).asInstanceOf[StdInteger]
    assertEquals(stdInteger.get(), intData)
    assertSame(stdInteger.asInstanceOf[PlatformData].getUnderlyingData, intData)
  }

  @Test
  def testCreateSparkLong(): Unit = {
    val longData = new lang.Long(1234L) // scalastyle:ignore magic.number
    val stdLong = SparkWrapper.createStdData(longData, DataTypes.LongType).asInstanceOf[StdLong]
    assertEquals(stdLong.get(), longData)
    assertSame(stdLong.asInstanceOf[PlatformData].getUnderlyingData, longData)
  }

  @Test
  def testCreateSparkBoolean(): Unit = {
    val booleanData = new lang.Boolean(true)
    val stdBoolean = SparkWrapper.createStdData(booleanData, DataTypes.BooleanType).asInstanceOf[StdBoolean]
    assertEquals(stdBoolean.get(), true)
    assertSame(stdBoolean.asInstanceOf[PlatformData].getUnderlyingData, booleanData)
  }

  @Test
  def testCreateSparkString(): Unit = {
    val stringData = UTF8String.fromString("test")
    val stdString = SparkWrapper.createStdData(stringData, DataTypes.StringType).asInstanceOf[StdString]
    assertEquals(stdString.get(), "test")
    assertSame(stdString.asInstanceOf[PlatformData].getUnderlyingData, stringData)
  }

  @Test
  def testCreateSparkFloat(): Unit = {
    val floatData = new lang.Float(1.0f)
    val stdFloat = SparkWrapper.createStdData(floatData, DataTypes.FloatType).asInstanceOf[StdFloat]
    assertEquals(stdFloat.get(), 1.0f)
    assertSame(stdFloat.asInstanceOf[PlatformData].getUnderlyingData, floatData)
  }

  @Test
  def testCreateSparkDouble(): Unit = {
    val doubleData = new lang.Double(2.0)
    val stdDouble = SparkWrapper.createStdData(doubleData, DataTypes.DoubleType).asInstanceOf[StdDouble]
    assertEquals(stdDouble.get(), 2.0)
    assertSame(stdDouble.asInstanceOf[PlatformData].getUnderlyingData, doubleData)
  }

  @Test
  def testCreateSparkBinary(): Unit = {
    val bytesData = ByteBuffer.wrap("foo".getBytes(Charset.forName("UTF-8")))
    val stdByte = SparkWrapper.createStdData(bytesData.array(), DataTypes.BinaryType).asInstanceOf[StdBinary]
    assertEquals(stdByte.get(), bytesData)
    assertSame(stdByte.asInstanceOf[PlatformData].getUnderlyingData, bytesData.array())
  }

}
