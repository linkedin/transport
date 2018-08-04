package portable_udfs.examples.nestedsum

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import portable_udfs.spark.util


/**
  * Spark wrapper for NestedSumPortableUDF
  * This wrapper will be auto-generated
  */
case class NestedSumExpr(s: Expression) extends Expression with CodegenFallback {
  private val _udf = new NestedSumPortableUDF()

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val col = s.eval(input)
    // warp platform specific data as standard data
    val stdInput = util.wrapData(col, s.dataType)
    // call portable udf
    val stdOutput = _udf.eval(stdInput)
    // unwrap potentially modified platform specific data
    stdOutput.getUnderlyingData
  }

  override def dataType: DataType = {
    // similarly wrap platform specific types with standard typed
    val stdInputType = util.wrapType(s.dataType)
    // determine output type for the platform
    val stdOutputType = _udf.getOutputType(stdInputType)
    // return the platform specific type
    stdOutputType.underlyingType.asInstanceOf[DataType]
  }

  override def children: Seq[Expression] = Seq(s)
}
