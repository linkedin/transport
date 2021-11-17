package udfs.spark

import java.util
import com.google.common.collect.ImmutableList
import com.linkedin.transport.api.udf.{UDF, TopLevelUDF}
import com.linkedin.transport.spark.{SparkStdUDF, StdUDFRegistration, StdUdfWrapper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression


case class SimpleUDF(expressions: Seq[Expression]) extends StdUdfWrapper(expressions) {

  override protected def getTopLevelUdfClass: Class[_ <: TopLevelUDF] = classOf[udfs.SimpleUDF]

  override protected def getStdUdfImplementations: util.List[_ <: UDF] = ImmutableList.of(
    new udfs.SimpleUDF()
  )
}

object SimpleUDF {

  def register(name: String): SparkStdUDF = {
    StdUDFRegistration.register(name, classOf[SimpleUDF])
  }

  def register(name: String, session: SparkSession): SparkStdUDF = {
    StdUDFRegistration.register(name, classOf[SimpleUDF], session)
  }
}
