package portable_udfs.spark.types

import java.util.{List => JavaList}

import org.apache.spark.sql.types._
import portable_udfs.api.types.StdStructType.StdFieldType
import portable_udfs.api.types._
import portable_udfs.spark._

import scala.collection.JavaConverters._

case class StdSparkLongType(t: DataType) extends StdLongType {
  override def underlyingType(): DataType = t
}

case class StdSparkMapType(k: DataType, v: DataType) extends StdMapType {
  override def asMapType(): StdSparkMapType = this

  override def underlyingType(): MapType = new MapType(k, v, true)

  override def keyType(): StdType = util.wrapType(k)

  override def valueType(): StdType = util.wrapType(v)
}

case class StdSparkStringType(s: DataType) extends StdStringType {
  override def underlyingType(): DataType = s
}

case class StdSparkStructType(s: StructType) extends StdStructType {

  override def underlyingType(): AnyRef = s

  override def fieldTypes(): JavaList[StdSparkFieldType] = {
    s.fields.map(f => new StdSparkFieldType(f)).toSeq.asJava
  }
}

class StdSparkFieldType(private val f: StructField) extends StdFieldType {
  override def name(): String = f.name
  override def stdType(): StdType = util.wrapType(f.dataType)

  /**
    * Get underlying StdFactory specific schema stdType.
    * For example if running on Spark, returns Spark specific schema types
    *
    * @return
    */
  override def underlyingType(): AnyRef = ???
}

case class StdSparkIntegerType(t: DataType) extends StdIntegerType {
  override def underlyingType(): AnyRef = t
}

case class StdSparkArrayType(elemType: DataType) extends  StdArrayType {
  override def getElemType: StdType = util.wrapType(elemType)
  override def underlyingType() = new ArrayType(elemType, true)
}
