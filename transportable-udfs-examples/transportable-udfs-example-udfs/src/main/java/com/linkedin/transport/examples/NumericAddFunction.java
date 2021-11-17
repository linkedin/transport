/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.linkedin.transport.api.udf.TopLevelUDF;


public interface NumericAddFunction extends TopLevelUDF {

  @Override
  default String getFunctionName() {
    return "numeric_add";
  }

  @Override
  default String getFunctionDescription() {
    return "Adds two integers, longs, reals, or doubles";
  }
}
