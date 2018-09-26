/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.common;

import java.util.Arrays;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.Assert;


public class AssertHiveGenericUdf {
  private AssertHiveGenericUdf() {
  }

  public static void assertFunction(GenericUDF udf, ObjectInspector[] objectInspectors, Object[] arguments,
      Object expected) {
    try {
      udf.initialize(objectInspectors);
    } catch (UDFArgumentException e) {
      throw new RuntimeException("Error initializing UDF: " + udf.getClass() + ".", e);
    }
    udf.getRequiredFiles();
    GenericUDF.DeferredObject[] deferredObjects =
        Arrays.stream(arguments)
            .map(object -> new GenericUDF.DeferredJavaObject(object))
            .toArray(GenericUDF.DeferredObject[]::new);
    try {
      Object result = udf.evaluate(deferredObjects);
      Assert.assertEquals(result, expected);
    } catch (HiveException e) {
      throw new RuntimeException("Error evaluating UDF: " + udf.getClass() + ".", e);
    }
  }
}
