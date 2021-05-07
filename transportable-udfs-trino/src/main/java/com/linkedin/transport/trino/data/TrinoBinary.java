/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.data.StdBinary;
import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import java.nio.ByteBuffer;

import static io.trino.spi.type.VarbinaryType.*;

public class TrinoBinary extends TrinoData implements StdBinary {

  private Slice _slice;

  public TrinoBinary(Slice slice) {
    _slice = slice;
  }

  @Override
  public ByteBuffer get() {
    return _slice.toByteBuffer();
  }

  @Override
  public Object getUnderlyingData() {
    return _slice;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _slice = (Slice) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    VARBINARY.writeSlice(blockBuilder, _slice);
  }
}
