package portable_udfs.examples.profilesections

import portable_udfs.api.data.{StdArray, StdData, StdMap}
import portable_udfs.spark.data.SparkPlatform

import scala.collection.mutable

class PortableGetProfileSectionUDF {
  // This will likely we loaded through ServiceLoader
  // running on the target platform
  val _platform = new SparkPlatform

  def apply(fields: StdMap, fieldsOrder: StdArray): StdData = {
    // _stdFactory create platform specific array and wraps it with StdArray
    val r = _platform.createArray(fields.getType.asMapType.valueType, 0)

    // build a setUnderlyingData of fields
    val keySet = mutable.Set[String]()
    (0 until fieldsOrder.size()).foreach {
      i => keySet += fieldsOrder.get(i).toString
    }

    val fks = fields.keys()  // gives StdArray
    val fvs = fields.values() // gives StdArray
    val sz = fks.size()

    (0 until sz).foreach {
      i => {
        val fk = fks.get(i)  // StdString (but would likely change to simple String in the future)
        val fv = fvs.get(i)  // StdStruct which wraps the platform specific struct
        if (keySet.contains(fk.toString)) {
          // Notice only the key `fk` is converted (to string), which also would be the
          // case if we had written this UDF in the target platform (Spark).
          // The value `fv` which is a complex Struct in the target platform is not converted
          // and is simply wrapped in a StdStruct API.

          // we add the stdstruct `fv` here which gets unwrapped in the target platform and the underlying platform specific struct is added without any conversion involved
          r.add(fv)
        }
      }
    }
    r
  }
}
