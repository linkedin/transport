/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.presto;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.ArrayElementAtFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class ArrayElementAtFunctionWrapper extends StdUdfWrapper {
  public ArrayElementAtFunctionWrapper() {
    super(new ArrayElementAtFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new ArrayElementAtFunction();
  }
}
