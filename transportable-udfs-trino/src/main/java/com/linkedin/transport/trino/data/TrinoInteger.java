/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.data.StdInteger;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.IntegerType.*;


public class TrinoInteger extends TrinoData implements StdInteger {

  int _integer;

  public TrinoInteger(int integer) {
    _integer = integer;
  }

  @Override
  public int get() {
    return _integer;
  }

  @Override
  public Object getUnderlyingData() {
    return _integer;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _integer = ((Long) value).intValue();
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    // It looks a bit strange, but the call to writeLong is correct here. INTEGER does not have a writeInt method for
    // some reason. It uses BlockBuilder.writeInt internally.
    INTEGER.writeLong(blockBuilder, _integer);
  }
}
