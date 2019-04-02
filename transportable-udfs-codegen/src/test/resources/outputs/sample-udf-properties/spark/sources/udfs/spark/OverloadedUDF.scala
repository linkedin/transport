package udfs.spark

import java.util
import com.google.common.collect.ImmutableList
import com.linkedin.transport.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.transport.spark.StdUdfWrapper
import org.apache.spark.sql.catalyst.expressions.Expression


case class OverloadedUDF(expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF] = classOf[udfs.OverloadedUDF]

  override protected def getStdUdfImplementations: util.List[_ <: StdUDF] = ImmutableList.of(
    new udfs.OverloadedUDFInt(), new udfs.OverloadedUDFString()
  )
}
