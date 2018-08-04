package portable_udfs.examples.nestedtransform

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import portable_udfs.spark.util

/**
  * Spark wrapper for NestedTransformPortableUDF 
  * This wrapper will be auto-generated
  */
case class NestedTransformExpr(s: Expression) extends Expression with CodegenFallback {
  private val _udf = new NestedTransformPortableUDF()

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val col = s.eval(input)
    // warp platform specific data as standard data
    val stdInput = util.wrapData(col, s.dataType)
    // call portable udf
    val stdOutput = _udf.eval(stdInput)
    // return modified underlying platform specific data
    stdOutput.getUnderlyingData
  }

  override def dataType: DataType = {
    val stdInputType = util.wrapType(s.dataType)
    val stdOutputType = _udf.getOutputType(stdInputType)
    stdOutputType.underlyingType().asInstanceOf[DataType]
  }

  override def children: Seq[Expression] = Seq(s)
}
