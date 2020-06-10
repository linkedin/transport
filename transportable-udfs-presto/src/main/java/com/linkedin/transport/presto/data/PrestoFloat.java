/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.linkedin.transport.api.data.StdFloat;
import io.prestosql.spi.block.BlockBuilder;


public class PrestoFloat extends PrestoData implements StdFloat {

  private float _float;

  public PrestoFloat(float aFloat) {
    _float = aFloat;
  }

  @Override
  public float get() {
    return _float;
  }

  @Override
  public Object getUnderlyingData() {
    return _float;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _float = (float) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    blockBuilder.writeInt(Float.floatToIntBits(_float));
  }
}
