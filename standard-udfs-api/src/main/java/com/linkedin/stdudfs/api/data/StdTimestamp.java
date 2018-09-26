/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.api.data;

/** A Standard UDF data type for representing timestamps. */
public interface StdTimestamp extends StdData {

  /** Returns the number of milliseconds elapsed from epoch for the {@link StdTimestamp}. */
  long toEpoch();
}
