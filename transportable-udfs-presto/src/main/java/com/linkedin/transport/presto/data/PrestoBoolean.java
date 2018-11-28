/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.facebook.presto.spi.block.BlockBuilder;
import com.linkedin.transport.api.data.StdBoolean;

import static com.facebook.presto.spi.type.BooleanType.*;


public class PrestoBoolean extends PrestoData implements StdBoolean {

  boolean _value;

  public PrestoBoolean(boolean value) {
    _value = value;
  }

  @Override
  public boolean get() {
    return _value;
  }

  @Override
  public Object getUnderlyingData() {
    return _value;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _value = (boolean) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    BOOLEAN.writeBoolean(blockBuilder, _value);
  }
}
