package udfs.spark

import java.util
import com.google.common.collect.ImmutableList
import com.linkedin.transport.api.udf.{StdUDF, TopLevelStdUDF}
import com.linkedin.transport.spark.StdUdfWrapper
import org.apache.spark.sql.catalyst.expressions.Expression


case class OverloadedUDF(expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelStdUDF] = classOf[udfs.OverloadedUDF]

  override protected def getStdUdfImplementations: util.List[_ <: StdUDF] = {
    ImmutableList.builder[StdUDF]()
      .add(new udfs.OverloadedUDF1(), new udfs.OverloadedUDF2(), new udfs.OverloadedUDF3(), new udfs.OverloadedUDF4(), new udfs.OverloadedUDF5(), new udfs.OverloadedUDF6(), new udfs.OverloadedUDF7(), new udfs.OverloadedUDF8(), new udfs.OverloadedUDF9(), new udfs.OverloadedUDF10())
      .build()
  }
}
