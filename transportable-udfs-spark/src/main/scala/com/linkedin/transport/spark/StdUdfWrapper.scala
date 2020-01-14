/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.spark

import java.io.{IOException, ObjectStreamException}
import java.nio.file.Paths
import java.util.List

import com.linkedin.transport.api.StdFactory
import com.linkedin.transport.api.data.{PlatformData, StdData}
import com.linkedin.transport.api.udf._
import com.linkedin.transport.spark.typesystem.SparkTypeInference
import com.linkedin.transport.utils.FileSystemUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

abstract class StdUdfWrapper(_expressions: Seq[Expression]) extends Expression
  with CodegenFallback with Serializable {

  @transient private var _stdFactory: StdFactory = _
  @transient private var _stdUdf: StdUDF = _
  @transient private var _requiredFilesProcessed: Boolean = false
  @transient private var _outputDataType: DataType = _
  private var _nullableArguments: Array[Boolean] = _
  private var _distributedCacheFiles: Array[String] = _

  override def nullable: Boolean = true

  override def dataType: DataType = {
    if (_outputDataType != null) _outputDataType else initialize()
  }

  private def initialize(): DataType = {
    val sparkTypeInference = new SparkTypeInference
    sparkTypeInference.compile(children.map(_.dataType).toArray, getStdUdfImplementations, getTopLevelUdfClass)
    _stdFactory = sparkTypeInference.getStdFactory
    _stdUdf = sparkTypeInference.getStdUdf
    _nullableArguments = _stdUdf.getAndCheckNullableArguments
    _stdUdf.init(_stdFactory)
    getRequiredFiles()
    _requiredFilesProcessed = false
    _outputDataType = sparkTypeInference.getOutputDataType
    _outputDataType
  }

  override def children: Seq[Expression] = _expressions


  // Suppressing magic number warming since the number match is required to cast it into the coresponding StdUDF
  // scalastyle:off magic.number
  private def getRequiredFiles(): Unit = { // scalastyle:ignore cyclomatic.complexity
    if (_distributedCacheFiles == null) {
      val wrappedConstants = checkNullsAndWrapConstants()
      // If wrappedConstants is null, it means there were non-nullable constants whose value was evaluated to be null
      // Hence we do not call user's getRequiredFiles(). Also in such a case, null checks in eval will also fail and
      // user's eval will never be called, so there is no need to getRequiredFiles() anyway.
      if (wrappedConstants != null) {
        val requiredFiles = wrappedConstants.length match {
          case 0 =>
            _stdUdf.asInstanceOf[StdUDF0[StdData]].getRequiredFiles()
          case 1 =>
            _stdUdf.asInstanceOf[StdUDF1[StdData, StdData]].getRequiredFiles(wrappedConstants(0))
          case 2 =>
            _stdUdf.asInstanceOf[StdUDF2[StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1))
          case 3 =>
            _stdUdf.asInstanceOf[StdUDF3[StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1), wrappedConstants(2))
          case 4 =>
            _stdUdf.asInstanceOf[StdUDF4[StdData, StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1), wrappedConstants(2), wrappedConstants(3))
          case 5 =>
            _stdUdf.asInstanceOf[StdUDF5[StdData, StdData, StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1), wrappedConstants(2), wrappedConstants(3), wrappedConstants(4))
          case 6 =>
            _stdUdf.asInstanceOf[StdUDF6[StdData, StdData, StdData, StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1), wrappedConstants(2), wrappedConstants(3), wrappedConstants(4), wrappedConstants(5))
          case 7 =>
            _stdUdf.asInstanceOf[StdUDF7[StdData, StdData, StdData, StdData, StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1), wrappedConstants(2), wrappedConstants(3), wrappedConstants(4), wrappedConstants(5), wrappedConstants(6))
          case 8 =>
            _stdUdf.asInstanceOf[StdUDF8[StdData, StdData, StdData, StdData, StdData, StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
              wrappedConstants(1), wrappedConstants(2), wrappedConstants(3), wrappedConstants(4), wrappedConstants(5), wrappedConstants(6), wrappedConstants(7))
          case _ =>
            throw new UnsupportedOperationException("getRequiredFiles not yet supported for StdUDF" + _expressions.length)
        }

        lazy val sparkContext = SparkSession.builder().getOrCreate().sparkContext
        _distributedCacheFiles = requiredFiles.map(file => {
          try {
            val resolvedFile = FileSystemUtils.resolveLatest(file)
            // TODO: Currently does not support adding of files with same file name. E.g dirA/file.txt dirB/file.txt
            sparkContext.addFile(resolvedFile)
            resolvedFile
          } catch {
            case e: IOException =>
              throw new RuntimeException("Failed to resolve path: [" + file + "].", e)
          }
        })
      }
    }
  } // scalastyle:on magic.number

  private final def checkNullsAndWrapConstants(): Array[StdData] = {
    val wrappedConstants = new Array[StdData](_expressions.length)
    for (i <- _expressions.indices) {
      val constantValue = if (_expressions(i).foldable) _expressions(i).eval() else null
      if (!_nullableArguments(i) && _expressions(i).foldable && constantValue == null) {
        // constant is defined as non nullable and value is null, so return early
        return null // scalastyle:ignore return
      }
      wrappedConstants(i) = SparkWrapper.createStdData(constantValue, _expressions(i).dataType)
    }
    wrappedConstants
  }

  // Suppressing magic number warming since the number match is required to cast it into the corresponding StdUDF
  // scalastyle:off magic.number
  override def eval(input: InternalRow): Any = { // scalastyle:ignore cyclomatic.complexity
    val wrappedArguments = checkNullsAndWrapArguments(input)
    // If wrappedArguments is null, it means there were non-nullable arguments whose value was evaluated to be null
    // So we do not call user's eval()
    if (wrappedArguments == null) {
      null
    } else {
      if (!_requiredFilesProcessed) {
        processRequiredFiles()
      }
      val stdResult = wrappedArguments.length match {
        case 0 =>
          _stdUdf.asInstanceOf[StdUDF0[StdData]].eval()
        case 1 =>
          _stdUdf.asInstanceOf[StdUDF1[StdData, StdData]].eval(wrappedArguments(0))
        case 2 =>
          _stdUdf.asInstanceOf[StdUDF2[StdData, StdData, StdData]].eval(wrappedArguments(0), wrappedArguments(1))
        case 3 =>
          _stdUdf.asInstanceOf[StdUDF3[StdData, StdData, StdData, StdData]].eval(wrappedArguments(0), wrappedArguments(1),
            wrappedArguments(2))
        case 4 =>
          _stdUdf.asInstanceOf[StdUDF4[StdData, StdData, StdData, StdData, StdData]].eval(wrappedArguments(0),
            wrappedArguments(1), wrappedArguments(2), wrappedArguments(3))
        case 5 =>
          _stdUdf.asInstanceOf[StdUDF5[StdData, StdData, StdData, StdData, StdData, StdData]].eval(wrappedArguments(0),
            wrappedArguments(1), wrappedArguments(2), wrappedArguments(3), wrappedArguments(4))
        case 6 =>
          _stdUdf.asInstanceOf[StdUDF6[StdData, StdData, StdData, StdData, StdData, StdData, StdData]].eval(wrappedArguments(0),
            wrappedArguments(1), wrappedArguments(2), wrappedArguments(3), wrappedArguments(4), wrappedArguments(5))
        case 7 =>
          _stdUdf.asInstanceOf[StdUDF7[StdData, StdData, StdData, StdData, StdData, StdData, StdData, StdData]].eval(wrappedArguments(0),
            wrappedArguments(1), wrappedArguments(2), wrappedArguments(3), wrappedArguments(4), wrappedArguments(5),
            wrappedArguments(6))
        case 8 =>
          _stdUdf.asInstanceOf[StdUDF8[StdData, StdData, StdData, StdData, StdData, StdData, StdData, StdData, StdData]].eval(wrappedArguments(0),
            wrappedArguments(1), wrappedArguments(2), wrappedArguments(3), wrappedArguments(4), wrappedArguments(5),
            wrappedArguments(6), wrappedArguments(7))
        case _ =>
          throw new UnsupportedOperationException("eval not yet supported for StdUDF" + _expressions.length)
      }

      if (stdResult == null) null else stdResult.asInstanceOf[PlatformData].getUnderlyingData
    }
  } // scalastyle:on magic.number


  private final def checkNullsAndWrapArguments(input: InternalRow): Array[StdData] = {
    val wrappedArguments = new Array[StdData](_expressions.length)
    for (i <- _expressions.indices) {
      val evaluatedExpression = _expressions(i).eval(input)
      if(!_nullableArguments(i) && evaluatedExpression == null) {
        // argument is defined as non nullable and value is null, so return early
        return null // scalastyle:ignore return
      }
      wrappedArguments(i) = SparkWrapper.createStdData(evaluatedExpression, _expressions(i).dataType)
    }
    wrappedArguments
  }

  private final def processRequiredFiles(): Unit = {
    if (!_requiredFilesProcessed) {
      val localFiles = _distributedCacheFiles.map(file => {
        try {
          SparkFiles.get(Paths.get(file).getFileName.toString)
        } catch {
          case e: IOException =>
            throw new RuntimeException("Failed to resolve path: [" + file + "].", e)
        }
      })
      _stdUdf.processRequiredFiles(localFiles)
      _requiredFilesProcessed = true
    }
  }

  protected def getStdUdfImplementations: List[_ <: StdUDF]

  protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF]

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val newInstance = super.makeCopy(newArgs).asInstanceOf[StdUdfWrapper]
    if (newInstance != null) {
      newInstance._stdFactory = _stdFactory
      newInstance._stdUdf = _stdUdf
      newInstance._requiredFilesProcessed = _requiredFilesProcessed
      newInstance._outputDataType = _outputDataType
      newInstance._nullableArguments = _nullableArguments
      newInstance._distributedCacheFiles = _distributedCacheFiles
    }
    newInstance
  }

  @throws(classOf[ObjectStreamException])
  private def readResolve(): Object = {
    initialize()
    this
  }
}
