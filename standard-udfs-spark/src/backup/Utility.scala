

package util {
  object Utility {
    def time[A](f: => A) = {
      val s = System.nanoTime
      val ret = f
      println("Time spent: " + (System.nanoTime - s) / 1e6 + " ms")
      // println((System.nanoTime - s) / 1e6)
      ret
    }
  }
}



package org.aapache.spark.sql.hive {
  // To access private spark.sql.hive apis we write the class under this pkg

  import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
  import org.apache.spark.sql.catalyst.expressions.Expression


  object HiveUtility {
    def mkFnBldr(name: String, clazz: Class[_]): FunctionBuilder = {
      (children: Seq[Expression]) => {
        throw new IllegalStateException("No handler for udf: " + name)
      }
    }
  }
}

package org.apache.spark.sql {
  // To access private spark.sql apis we write the class under this pkg

  import org.aapache.spark.sql.hive.HiveUtility
  import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
  import org.apache.spark.sql.catalyst.expressions.Expression

  import scala.reflect.ClassTag
  import scala.util.{Failure, Success, Try}

  object SessionUtility {
    def registerHiveUDF(spark: SparkSession, name: String, clazz: Class[_]) {
      spark.sessionState.functionRegistry.registerFunction(name, HiveUtility.mkFnBldr(name, clazz))
    }

    def registerSparkExpr[T <: Expression](spark: SparkSession, name: String, exprClazz: Class[T]): Unit = {
      spark.sessionState.functionRegistry.registerFunction(name, fnBldr(exprClazz))
    }

    def fnBldr[T <: Expression](exprClazz: Class[T]): FunctionBuilder = {
      (children: Seq[Expression]) => {
        val params = Seq.fill(children.size)(classOf[Expression])
        val tag = ClassTag[T](exprClazz)
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params: _*)) match {
          case Success(e) => e
          case Failure(e) => throw new IllegalStateException(e)
        }
        Try(f.newInstance(children: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new IllegalStateException(e.getCause.getMessage)
        }
      }
    }
  }
}
