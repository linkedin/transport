package portable_udfs.spark.data

import java.util.{List => JavaList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import portable_udfs.api.Platform
import portable_udfs.api.data._
import portable_udfs.api.types.StdStructType.StdFieldType
import portable_udfs.api.types._
import portable_udfs.spark.types._

class SparkPlatform extends Platform {
  override def createArray(elementType: StdType, size: Int): StdArray =
    new StdSparkArray(
      new GenericArrayData(new Array(size)),
      new ArrayType(elementType.underlyingType().asInstanceOf[DataType], containsNull = true))

  override def createMap(keyType: StdType, valueType: StdType): StdMap = {
    //TODO: make these as separate mutable standard spark types
    new StdSparkMap(
      new ArrayBasedMapData(
        new GenericArrayData(Array.empty[Any]),
        new GenericArrayData(Array.empty[Any])),
      new MapType(
        keyType.underlyingType().asInstanceOf[DataType],
        valueType.underlyingType().asInstanceOf[DataType], true)
    )
  }

  override def createStruct(fieldTypes: JavaList[StdFieldType]): StdStruct = {
    //TODO: make these as separate mutable standard spark types
    new StdSparkStruct(
      new GenericInternalRow(Array.empty[Any]),
      new StructType()
    )
  }

  override def createIntType(): StdIntegerType = StdSparkIntegerType(DataTypes.IntegerType)

  override def createLongType(): StdLongType = StdSparkLongType(DataTypes.LongType)

  override def createStringType(): StdStringType = StdSparkStringType(DataTypes.StringType)


  override def createArrayType(elemType: StdType): StdArrayType = {
    StdSparkArrayType(elemType.underlyingType().asInstanceOf[DataType])
  }

  override def createMapType(keyType: StdType, valType: StdType): StdMapType = {
    StdSparkMapType(
      keyType.underlyingType().asInstanceOf[DataType],
      valType.underlyingType().asInstanceOf[DataType])
  }

  override def createStructType(fieldTypes: JavaList[StdFieldType]): StdStructType = {
    val r = new Array[StructField](fieldTypes.size())
    (0 until fieldTypes.size()).foreach {
      idx => {
        val ft = fieldTypes.get(idx)
        r(idx) = StructField(
          ft.name,
          ft.stdType.underlyingType.asInstanceOf[DataType])
      }
    }
    StdSparkStructType(StructType(r))
  }

  override def createInt(data: Int): StdInteger = StdSparkInteger(data)

  override def createLong(data: Long): StdLong = StdSparkLong(data)

  override def createString(data: String): StdString = StdSparkString(UTF8String.fromString(data))

}
