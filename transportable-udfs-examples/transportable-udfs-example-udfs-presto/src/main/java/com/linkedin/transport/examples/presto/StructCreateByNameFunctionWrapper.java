/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.examples.StructCreateByNameFunction;
import com.linkedin.transport.presto.StdUdfWrapper;


public class StructCreateByNameFunctionWrapper extends StdUdfWrapper {
  public StructCreateByNameFunctionWrapper() {
    super(new StructCreateByNameFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new StructCreateByNameFunction();
  }
}
