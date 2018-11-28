/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.examples.StructCreateByIndexFunction;
import com.linkedin.transport.presto.StdUdfWrapper;


public class StructCreateByIndexFunctionWrapper extends StdUdfWrapper {
  public StructCreateByIndexFunctionWrapper() {
    super(new StructCreateByIndexFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new StructCreateByIndexFunction();
  }
}
