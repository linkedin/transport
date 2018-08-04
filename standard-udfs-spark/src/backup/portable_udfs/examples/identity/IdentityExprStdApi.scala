package portable_udfs.examples.identity

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType
import portable_udfs.spark.data.StdSparkInteger

/**
  * Created by rratti on 9/7/17.
  */
case class IdentityExprStdApi(c: Expression) extends UnaryExpression with CodegenFallback {
  //val _udf = new IdentityFn

  override def eval(input: InternalRow): Any = {
    val i = child.eval(input)
    //val r = _udf.apply(StdSparkInteger(i.asInstanceOf[Integer]))
    //r.getUnderlyingData
  }

  override def child: Expression = c

  override def dataType: DataType = c.dataType
}
