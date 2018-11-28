/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.facebook.presto.spi.block.BlockBuilder;
import com.linkedin.transport.api.data.StdInteger;

import static com.facebook.presto.spi.type.IntegerType.*;


public class PrestoInteger extends PrestoData implements StdInteger {

  int _integer;

  public PrestoInteger(int integer) {
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
