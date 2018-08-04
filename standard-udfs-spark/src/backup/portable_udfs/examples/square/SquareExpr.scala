package portable_udfs.examples.square

import portable_udfs.spark.util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
  * This wrapper will be auto-generated
  */
case class SquareExpr(s: Expression) extends UnaryExpression with CodegenFallback {
  private val _udf = new SquarePortableUDF()

  override def eval(input: InternalRow): Any = {
    val col = s.eval(input)
    // warp platform specific data as standard data
    val stdInput = util.wrapData(col, s.dataType)
    // call portable udf
    val stdOutput = _udf.eval(stdInput)
    // return modified underlying platform specific data
    stdOutput.getUnderlyingData
  }

  override def child: Expression = s

  override def dataType: DataType = {
    // similarly wrap platform specific types with standard typed
    val stdInputType = util.wrapType(s.dataType)
    // determine output type for the platform
    val stdOutputType = _udf.getOutputType(stdInputType)
    // return the platform specific type
    stdOutputType.underlyingType.asInstanceOf[DataType]
  }
}
