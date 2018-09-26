/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.presto;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapFromTwoArraysFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class MapFromTwoArraysFunctionWrapper extends StdUdfWrapper {
  public MapFromTwoArraysFunctionWrapper() {
    super(new MapFromTwoArraysFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapFromTwoArraysFunction();
  }
}
