/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public interface NumericAddFunction extends TopLevelStdUDF {

  @Override
  default String getFunctionName() {
    return "numeric_add";
  }

  @Override
  default String getFunctionDescription() {
    return "Adds two integers, longs, floats, or doubles";
  }
}
