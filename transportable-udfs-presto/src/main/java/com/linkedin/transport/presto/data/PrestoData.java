/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.data;

import com.linkedin.transport.api.data.PlatformData;
import io.prestosql.spi.block.BlockBuilder;


/**
 * A common super class for all Presto specific implementations of StdData types
 */
public abstract class PrestoData implements PlatformData {
  /**
   * Writes this data object into the give BlockBuilder
   * @param blockBuilder the builder to write into
   */
  public abstract void writeToBlock(BlockBuilder blockBuilder);
}
