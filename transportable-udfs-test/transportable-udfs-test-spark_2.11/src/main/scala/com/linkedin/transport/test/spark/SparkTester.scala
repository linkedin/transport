/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spark

import com.linkedin.transport.api.{TypeFactory, types}

import java.util
import com.linkedin.transport.api.udf.{TopLevelUDF, UDF}
import com.linkedin.transport.spark.SparkTypeFactory
import com.linkedin.transport.spark.typesystem.SparkBoundVariables
import com.linkedin.transport.test.spi.{SqlFunctionCallGenerator, SqlTester, ToPlatformTestOutputConverter}
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, UDFTestUtils}
import org.testng.Assert

import scala.collection.JavaConversions._

class SparkTester extends SqlTester {

  private val _typeFactory: TypeFactory = new SparkTypeFactory(new SparkBoundVariables())
  private val _sparkSession: SparkSession = SparkSession.builder.master("local[1]").appName("transport-udfs").getOrCreate
  private val _sqlFunctionCallGenerator = new SparkSqlFunctionCallGenerator
  private val _testDataToOutputDataConverter = new ToSparkTestOutputConverter


  override def getTypeFactory: TypeFactory = _typeFactory

  override def getSqlFunctionCallGenerator: SqlFunctionCallGenerator = _sqlFunctionCallGenerator

  override def getToPlatformTestOutputConverter: ToPlatformTestOutputConverter = _testDataToOutputDataConverter

  override def assertFunctionCall(functionCallString: String, expectedOutputData: AnyRef,
    expectedOutputType: AnyRef): Unit = {
    val result =
      try {
        _sparkSession.sql("SELECT " + functionCallString).first
      } catch {
        case e: SparkException => throw e.getCause()
      }
    Assert.assertEquals(result.get(0), expectedOutputData)
    Assert.assertEquals(getModifiedResultType(result.schema.head.dataType), expectedOutputType)
  }

  override def setup(topLevelUDFClassesAndImplementations: util.Map[Class[_ <: TopLevelUDF],
    util.List[Class[_ <: UDF]]]): Unit = {
    topLevelUDFClassesAndImplementations.toMap.foreach(entry => {
      val functionName = entry._2.get(0).getConstructor().newInstance().asInstanceOf[TopLevelUDF].getFunctionName
      UDFTestUtils.register(functionName, entry._1, entry._2, _sparkSession)
    })
  }

  /**
    * Returns a modified [[DataType]] from the Spark query where nullability information is always set to true.
    * This is because the nullability information in the output [[DataType]] received from Spark is set based on the
    * runtime input values e.g. ARRAY(1, 2) will result in a [[DataType]] with `containsNull = false`.
    * [[types.DataType]] doesn't have a way to define nullability information for complex types.
    */
  private def getModifiedResultType(dataType: DataType): DataType = {
    dataType match {
      case a: ArrayType => ArrayType(getModifiedResultType(a.elementType), containsNull = true)
      case m: MapType => MapType(getModifiedResultType(m.keyType), getModifiedResultType(m.valueType),
        valueContainsNull = true)
      case s: StructType => StructType(s.map(x => StructField(x.name, getModifiedResultType(x.dataType),
        nullable = true)))
      case _ => dataType
    }
  }
}

