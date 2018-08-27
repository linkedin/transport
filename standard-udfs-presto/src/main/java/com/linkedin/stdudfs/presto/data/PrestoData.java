package com.linkedin.stdudfs.presto.data;

import com.facebook.presto.spi.block.BlockBuilder;
import com.linkedin.stdudfs.api.data.PlatformData;


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
