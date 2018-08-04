package com.linkedin.stdudfs.spark

import java.io.{IOException, ObjectStreamException}
import java.nio.file.Paths
import java.util.List

import com.linkedin.stdudfs.api.StdFactory
import com.linkedin.stdudfs.api.data.{PlatformData, StdData}
import com.linkedin.stdudfs.api.udf._
import com.linkedin.stdudfs.spark.typesystem.SparkTypeInference
import com.linkedin.stdudfs.utils.FileSystemUtils
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

  private def getRequiredFiles(): Unit = {
    if (_distributedCacheFiles == null) {
      val wrappedConstants = wrapConstants()
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
        case 4 => // scalastyle:ignore magic.number
          _stdUdf.asInstanceOf[StdUDF4[StdData, StdData, StdData, StdData, StdData]].getRequiredFiles(wrappedConstants(0),
            wrappedConstants(1), wrappedConstants(2), wrappedConstants(3))
        case _ =>
          throw new UnsupportedOperationException("getRequiredFiles not yet supported for StdUDF" + _expressions.length)
      }

      lazy val sparkContext = SparkSession.builder().getOrCreate().sparkContext
      _distributedCacheFiles = requiredFiles.map(file => {
        try {
          val resolvedFile = FileSystemUtils.resolveLatest(file, FileSystemUtils.getHDFSFileSystem)
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

  private final def wrapConstants(): Seq[StdData] = {
    _expressions.map(expr => {
      if (expr.foldable) SparkWrapper.createStdData(expr.eval(), expr.dataType) else null
    })
  }

  override def eval(input: InternalRow): Any = {
    if (containsNullValuedNonNullableArgument(input)) {
      null
    } else {
      if (!_requiredFilesProcessed) {
        processRequiredFiles()
      }

      val wrappedArguments = wrapArguments(input)
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
        case 4 => // scalastyle:ignore magic.number
          _stdUdf.asInstanceOf[StdUDF4[StdData, StdData, StdData, StdData, StdData]].eval(wrappedArguments(0),
            wrappedArguments(1), wrappedArguments(2), wrappedArguments(3))
        case _ =>
          throw new UnsupportedOperationException("eval not yet supported for StdUDF" + _expressions.length)
      }

      if (stdResult == null) null else stdResult.asInstanceOf[PlatformData].getUnderlyingData
    }
  }

  private final def containsNullValuedNonNullableArgument(input: InternalRow): Boolean = {
    _nullableArguments.zip(_expressions).exists({ case (nullable, expression) => !nullable && expression.eval(input) == null })
  }

  private final def wrapArguments(input: InternalRow): Seq[StdData] = {
    _expressions.map(expr => SparkWrapper.createStdData(expr.eval(input), expr.dataType))
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
