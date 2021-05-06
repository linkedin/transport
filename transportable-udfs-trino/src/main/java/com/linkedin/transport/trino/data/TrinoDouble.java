/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.data.StdDouble;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.DoubleType.*;


public class TrinoDouble extends TrinoData implements StdDouble {

  private double _double;

  public TrinoDouble(double aDouble) {
    _double = aDouble;
  }

  @Override
  public double get() {
    return _double;
  }

  @Override
  public Object getUnderlyingData() {
    return _double;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _double = (double) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    DOUBLE.writeDouble(blockBuilder, _double);
  }
}