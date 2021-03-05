/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.data.StdFloat;
import io.trino.spi.block.BlockBuilder;

import static java.lang.Float.*;


public class TrinoFloat extends TrinoData implements StdFloat {

  private float _float;

  public TrinoFloat(float aFloat) {
    _float = aFloat;
  }

  @Override
  public float get() {
    return _float;
  }

  @Override
  public Object getUnderlyingData() {
    return (long) floatToIntBits(_float);
  }

  @Override
  public void setUnderlyingData(Object value) {
    _float = (float) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    blockBuilder.writeInt(floatToIntBits(_float));
  }
}
